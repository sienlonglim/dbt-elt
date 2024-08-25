from dagster import file_relative_path
import os


DBT_PROJECT_DIR = file_relative_path(__file__, "../../dbt_elt")
DBT_MANIFEST_PATH = os.path.join(DBT_PROJECT_DIR, "target", "manifest.json")

DUCKDB_TARGET = 'dev'
DUCKDB_DIR = f"{DBT_PROJECT_DIR}/{DUCKDB_TARGET}.duckdb"
DUCKDB_SCHEMA = "raw"

USERNAME = os.getenv("username")

API_RESOURCE_MAPPER = {
    "hdb_resale_prices": "https://data.gov.sg/api/action/datastore_search?resource_id=d_8b84c4ee58e3cfc0ece0d773c8ca6abc",
    "air_temperature": "https://api.data.gov.sg/v1/environment/air-temperature"
}
