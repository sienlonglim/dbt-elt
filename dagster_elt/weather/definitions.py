from dagster import (
    Definitions
)

from .assets import (
    create_schema_and_table,
    get_historical_station_metadata,
    get_historical_half_hourly_air_temperature_readings
)
from ..dagster_utils.constants import (
    DUCKDB_TARGET,
    DUCKDB_DIR,
    DUCKDB_SCHEMA
)
from ..dagster_utils.resources import (
    DuckDbConfig,
    datagov_api_resource,
    duckdb_resource,
    dbt_resource
)


defs = Definitions(
    assets=[
        create_schema_and_table,
        get_historical_station_metadata,
        get_historical_half_hourly_air_temperature_readings
    ],
    resources={
        "duckdb_config": DuckDbConfig(
            target_database=DUCKDB_TARGET,
            directory=DUCKDB_DIR,
            database_schema=DUCKDB_SCHEMA
        ),
        "datagov_api": datagov_api_resource,
        "duckdb": duckdb_resource,
        "dbt": dbt_resource,
    }
)
