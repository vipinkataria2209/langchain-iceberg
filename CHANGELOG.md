# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Phase 1: Core Toolkit**
  - Initial project setup with core toolkit structure
  - `IcebergToolkit` main class for LangChain integration
  - Catalog exploration tools:
    - `ListNamespacesTool` - List all namespaces in catalog
    - `ListTablesTool` - List tables in a namespace
    - `GetSchemaTool` - Get table schema with sample data
  - Query execution tools:
    - `QueryTool` - Execute queries with filters and column selection
  - Utility modules:
    - `FilterBuilder` - Parse filter expressions to PyArrow
    - `ResultFormatter` - Format query results for display
    - Input validators for table_id, namespace, and filters
  - Custom exception hierarchy for better error handling
  - Basic test suite
  - Example usage scripts
  - Documentation (README, LICENSE)

- **Phase 2: Time-Travel Features**
  - `SnapshotTool` - List table snapshots with timestamps and operations
  - `TimeTravelTool` - Query historical data at specific snapshots or timestamps
  - Support for ISO 8601 timestamp format
  - Snapshot ID-based queries

- **Phase 3: Semantic Layer**
  - `SemanticLoader` - YAML configuration loader and parser
  - YAML schema validation with comprehensive error messages
  - `MetricToolGenerator` - Auto-generates metric tools from YAML
  - `DateRangeParser` - Parses various date range formats (Q4_2024, last_30_days, etc.)
  - Support for metrics: sum, count, avg, min, max
  - Support for dimensions: categorical, datetime, numeric
  - Example semantic YAML file

- **Phase 4: Governance & Polish**
  - `AccessControlValidator` - Role-based access control for metrics and tables
  - `PIIMasker` - Automatic PII masking in query results
  - `AuditLogger` - Query and operation logging for compliance
  - `QueryPlannerTool` - LLM-assisted query planning
  - Query timeout support (configurable, default 60s)
  - Row limit enforcement (configurable, default 10000)
  - Governance configuration in semantic YAML

### Changed
- N/A

### Deprecated
- N/A

### Removed
- N/A

### Fixed
- N/A

### Security
- N/A

## [0.1.0] - TBD

### Added
- Initial release with core functionality
- Support for REST, Hive, Glue, and Nessie catalogs
- Basic query execution with filter support
- Schema discovery and table exploration

