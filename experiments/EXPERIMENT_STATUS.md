# EPA Air Quality Data Experiments - Status

## ✅ Completed

1. **Data Download** ✅
   - Downloaded all EPA daily summary files for 2019-2024
   - PM2.5 (parameter 88101) and Ozone (parameter 44201) data
   - 12 files total, all extracted to CSV format
   - Files located in `data/epa/`

2. **Experiment Framework Created** ✅
   - `epa_data_loader.py` - Data loader for EPA CSV files
   - `epa_semantic.yaml` - Semantic layer configuration
   - `evaluation_framework.py` - Evaluation framework
   - `run_experiments.py` - Main experiment runner
   - `download_epa_data.py` - Data downloader

3. **Docker Services** ✅
   - Iceberg REST catalog running
   - MinIO S3-compatible storage running

## ⚠️ In Progress / Issues

1. **Table Creation**
   - Need to configure S3 properties for PyIceberg table creation
   - Currently getting S3 connection errors when creating tables
   - Solution: Use Spark SQL or properly configure S3 in PyIceberg catalog

2. **NumPy Compatibility Warning**
   - NumPy 2.x compatibility warnings (non-blocking)
   - Can be resolved by downgrading to NumPy <2 if needed

## 📋 Next Steps

1. **Fix Table Creation**
   - Option A: Use Spark SQL to create table (more reliable)
   - Option B: Fix PyIceberg S3 configuration in catalog initialization

2. **Load Sample Data**
   - Load 1-2 CSV files as test
   - Verify data integrity

3. **Run Experiments**
   - Run evaluation framework with sample queries
   - Compare semantic layer vs direct access
   - Generate results

## 📊 Data Files Available

- `data/epa/daily_88101_2019.csv` - PM2.5 2019 (~130MB)
- `data/epa/daily_88101_2020.csv` - PM2.5 2020 (~130MB)
- `data/epa/daily_88101_2021.csv` - PM2.5 2021 (~130MB)
- `data/epa/daily_88101_2022.csv` - PM2.5 2022 (~130MB)
- `data/epa/daily_88101_2023.csv` - PM2.5 2023 (~130MB)
- `data/epa/daily_88101_2024.csv` - PM2.5 2024 (~130MB)
- `data/epa/daily_44201_2019.csv` - Ozone 2019 (~130MB)
- `data/epa/daily_44201_2020.csv` - Ozone 2020 (~130MB)
- `data/epa/daily_44201_2021.csv` - Ozone 2021 (~130MB)
- `data/epa/daily_44201_2022.csv` - Ozone 2022 (~130MB)
- `data/epa/daily_44201_2023.csv` - Ozone 2023 (~130MB)
- `data/epa/daily_44201_2024.csv` - Ozone 2024 (~130MB)

Total: ~1.5GB of EPA air quality data ready for loading.

## 🔧 Quick Fix for Table Creation

The table creation needs S3 configuration. Update the catalog initialization to include:

```python
catalog = load_catalog(
    name="rest",
    type="rest",
    uri="http://localhost:8181",
    warehouse="s3://warehouse/wh/",
    s3_endpoint="http://localhost:9000",
    s3_access_key_id="admin",
    s3_secret_access_key="password",
    s3_path_style_access="true",
)
```

Or use Spark SQL (see `scripts/create_epa_tables.sql`).

