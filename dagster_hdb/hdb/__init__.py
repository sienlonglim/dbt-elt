from dagster import (
    AssetSelection,
    Definitions, 
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules
)
from dagster_duckdb import DuckDBResource
from dagster_duckdb_pandas import DuckDBPandasIOManager
from . import assets
from .config import *
from .resources import *


all_assets = load_assets_from_modules([assets])

# Jobs
hdb_resales_job = define_asset_job("hdb_resales_job", selection=AssetSelection.all())

# Schedules
hdb_resales_schedule = ScheduleDefinition(
    job=hdb_resales_job,
    cron_schedule="* * * * *"
)

defs = Definitions(
    assets=all_assets,
    resources={
        "conn": DataGovResourceAPI(user="my_user"),
        "duckdb": DuckDBResource(database=DUCKDB_DIR),
        "io_manager": DuckDBPandasIOManager(
            database=DUCKDB_DIR, 
            schema=SCHEMA
        )
    },
    schedules=[hdb_resales_schedule]
)
