from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules
)
# from dagster_duckdb_pandas import DuckDBPandasIOManager
from dagster_dbt import DbtCliResource

from .hdb import hdb_assets
from .dbt import dbt_assets
from .weather import weather_assets
from .dagster_utils.resources import (
    DbtDuckDbConfig,
    CustomDuckDBResource,
    DataGovAPI,
    AmazonS3
)

from .dagster_utils.constants import (
    DBT_PROJECT_DIR,
    DBT_MANIFEST_PATH,
    DUCKDB_TARGET,
    DUCKDB_DIR,
    DUCKDB_SCHEMA,
    USERNAME
)


all_assets = load_assets_from_modules(
    [hdb_assets,
     weather_assets,
     dbt_assets
     ]
)

# Jobs
hdb_resales_job = define_asset_job(
    "hdb_resales_job",
    selection=[
        "get_hdb_resale_csv",
        "create_schema_table",
        "resale_prices"
    ]
)

weather_air_temperature_job = define_asset_job(
    "weather_air_temperature_job",
    selection=[
        "create_schema_and_table",
        "get_historical_station_metadata",
        "get_historical_half_hourly_air_temperature_readings"
    ]
)

# Schedules
hdb_resales_schedule = ScheduleDefinition(
    job=hdb_resales_job,
    cron_schedule="* * * * *"
)

weather_air_temperature_schedule = ScheduleDefinition(
    job=weather_air_temperature_job,
    cron_schedule="* * * * *"
)

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt_duckdb_config": DbtDuckDbConfig(
            TARGET=DUCKDB_TARGET,
            DBT_PROJECT_DIR=DBT_PROJECT_DIR,
            DBT_MANIFEST_PATH=DBT_MANIFEST_PATH,
            DUCKDB_DIR=DUCKDB_DIR,
            SCHEMA=DUCKDB_SCHEMA
        ),
        "datagov_api": DataGovAPI(user=USERNAME),
        "duckdb": CustomDuckDBResource(
            user=USERNAME,
            database=DUCKDB_DIR
        ),
        # "io_manager": DuckDBPandasIOManager(
        #     database=DUCKDB_DIR,
        #     schema=SCHEMA
        # ),
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR),
        "s3_client": AmazonS3()
    },
    schedules=[hdb_resales_schedule, weather_air_temperature_schedule]
)
