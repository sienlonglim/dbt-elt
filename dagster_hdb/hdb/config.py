from datetime import date, timedelta
from dagster import file_relative_path

TARGET = 'dev'
DBT_PROJECT_DIR = file_relative_path(__file__, "../../dbt_duckdb_dagster")
DUCKDB_DIR = f"{DBT_PROJECT_DIR}/{TARGET}.duckdb"
SCHEMA = "raw"

# Use current day to get previous month and year
current_date = date.today()
first_day = current_date.replace(day=1)
last_month = first_day - timedelta(days=1)

YEAR_MONTH = f'{str(last_month.year)}-{str(last_month.month).zfill(2)}'