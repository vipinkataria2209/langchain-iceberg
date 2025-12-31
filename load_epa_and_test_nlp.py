#!/usr/bin/env python3
"""
Load EPA data into Iceberg and test NLP queries.
Uses SQL catalog (same as test data) for consistency.
"""

import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings('ignore')

sys.path.insert(0, str(Path(__file__).parent))

from experiments.epa_data_loader import EPADataLoader
from pyiceberg.catalog import load_catalog
from langchain_iceberg import IcebergToolkit

def load_epa_data():
    """Load EPA data into Iceberg tables."""
    print("=" * 70)
    print("Loading EPA Data into Iceberg")
    print("=" * 70)
    
    # Use SQL catalog (same as test data)
    warehouse_path = Path("warehouse_sql").absolute()
    warehouse_path.mkdir(exist_ok=True)
    catalog_db = warehouse_path / "catalog.db"
    
    print(f"\n[1/4] Initializing SQL catalog...")
    print(f"   Warehouse: file://{warehouse_path}")
    print(f"   Catalog DB: {catalog_db}")
    
    try:
        catalog = load_catalog(
            name="sql",
            type="sql",
            uri=f"sqlite:///{catalog_db}",
            warehouse=f"file://{warehouse_path}",
        )
        print("✅ SQL catalog initialized")
    except Exception as e:
        print(f"❌ Error initializing catalog: {e}")
        return None
    
    # Create namespace
    print("\n[2/4] Creating 'epa' namespace...")
    try:
        catalog.create_namespace("epa")
        print("✅ Created 'epa' namespace")
    except Exception:
        print("✅ 'epa' namespace already exists")
    
    # Create loader
    print("\n[3/4] Creating EPA data loader...")
    loader = EPADataLoader(catalog, namespace="epa")
    
    # Create table
    try:
        try:
            catalog.drop_table(("epa", "daily_summary"))
            print("   Dropped existing table")
        except Exception:
            pass
        
        table = loader.create_table()
        print("✅ Table 'epa.daily_summary' created")
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # Load sample files (start with a few files to test)
    print("\n[4/4] Loading EPA data files...")
    data_dir = Path("data/epa")
    csv_files = sorted(list(data_dir.glob("daily_*.csv")))
    
    if not csv_files:
        print("❌ No CSV files found in data/epa/")
        return None
    
    print(f"   Found {len(csv_files)} CSV files")
    print("   Loading sample files (first 2 files for testing)...")
    
    total_rows = 0
    for i, csv_file in enumerate(csv_files[:2], 1):  # Load first 2 files
        print(f"\n   [{i}/2] Loading {csv_file.name}...")
        try:
            rows = loader.load_csv_file(str(csv_file), table)
            total_rows += rows
            print(f"      ✅ Loaded {rows:,} rows")
        except Exception as e:
            print(f"      ⚠️  Error loading {csv_file.name}: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n✅ Total rows loaded: {total_rows:,}")
    
    # Verify data
    try:
        scan = table.scan(limit=5)
        result = scan.to_arrow()
        print(f"✅ Verified: {len(result)} sample rows retrieved")
    except Exception as e:
        print(f"⚠️  Verification error: {e}")
    
    return catalog

def test_nlp_queries():
    """Test NLP queries with EPA data."""
    print("\n" + "=" * 70)
    print("Testing NLP Queries with EPA Data")
    print("=" * 70)
    
    # Check API key
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("❌ OPENAI_API_KEY not set")
        print("   Set it in .env file or export it")
        return
    
    print(f"✅ OpenAI API key configured\n")
    
    # Use SQL catalog
    warehouse_path = Path("warehouse_sql").absolute()
    catalog_db = warehouse_path / "catalog.db"
    
    if not catalog_db.exists():
        print("❌ No catalog found. Please load data first.")
        return
    
    # Initialize toolkit
    print("[1/3] Setting up toolkit...")
    toolkit = IcebergToolkit(
        catalog_name="sql",
        catalog_config={
            "type": "sql",
            "uri": f"sqlite:///{catalog_db}",
            "warehouse": f"file://{warehouse_path}",
        }
    )
    
    tools = toolkit.get_tools()
    print(f"✅ Toolkit ready with {len(tools)} tools")
    
    # Create agent
    print("\n[2/3] Creating LangChain agent...")
    try:
        from langchain_openai import ChatOpenAI
        from langchain.agents import create_react_agent, AgentExecutor
        from langchain import hub
        
        llm = ChatOpenAI(model="gpt-4", temperature=0, api_key=api_key)
        prompt = hub.pull("hwchase17/react")
        agent = create_react_agent(llm, tools, prompt=prompt)
        agent_executor = AgentExecutor(
            agent=agent,
            tools=tools,
            verbose=False,
            handle_parsing_errors=True,
            max_iterations=5
        )
        print("✅ Agent ready")
        
    except Exception as e:
        print(f"❌ Failed to create agent: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Test queries
    print("\n[3/3] Testing NLP queries:")
    print("=" * 70)
    
    questions = [
        "What namespaces are available?",
        "List all tables in the epa namespace",
        "Show me the schema of the epa.daily_summary table",
        "What columns are in the daily_summary table?",
        "Show me sample air quality data",
        "What is the average PM2.5 concentration?",
        "How many records are in the daily_summary table?",
        "What states have air quality data?",
    ]
    
    for i, question in enumerate(questions, 1):
        print(f"\n[Query {i}] {question}")
        print("-" * 70)
        try:
            result = agent_executor.invoke({"input": question})
            print(f"✅ Answer:\n{result['output']}\n")
        except Exception as e:
            print(f"❌ Error: {str(e)[:300]}\n")
    
    print("=" * 70)
    print("✅ Testing Complete")
    print("=" * 70)

def main():
    """Main function."""
    # Load EPA data
    catalog = load_epa_data()
    
    if catalog:
        # Test NLP queries
        test_nlp_queries()
    else:
        print("\n❌ Failed to load EPA data. Cannot test NLP queries.")

if __name__ == "__main__":
    main()

