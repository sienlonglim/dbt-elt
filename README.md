# Data Pipeline with Dagster and Dbt
This project aims to build a data pipeline with CI practices using
1. Dagster
   - Orchestration of data extraction and loading to S3
   - Data ingestion from S3 lake into MotherDuck (Cloud DWH)
2. DuckDB (MotherDuck)
   - Data Warehousing
3. Dbt
   - Data transformation
