from dagster import file_relative_path
import os

TARGET = 'dev'
DBT_PROJECT_DIR = file_relative_path(__file__, "../../dbt_duckdb_dagster")
DBT_MANIFEST_PATH = os.path.join(DBT_PROJECT_DIR, "target", "manifest.json")
DUCKDB_DIR = f"{DBT_PROJECT_DIR}/{TARGET}.duckdb"
SCHEMA = "raw"
USERNAME = os.getenv("username")


