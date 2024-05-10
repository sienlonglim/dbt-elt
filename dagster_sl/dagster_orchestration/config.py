from dagster import file_relative_path

TARGET = 'dev'
DBT_PROJECT_DIR = file_relative_path(__file__, "../../dbt_duckdb_dagster")
DUCKDB_DIR = f"{DBT_PROJECT_DIR}/{TARGET}.duckdb"
SCHEMA = "raw"



