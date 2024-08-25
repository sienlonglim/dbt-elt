from dagster import (
    Definitions,
    EnvVar
)
from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource

from .assets import (
    create_schema_and_table,
    get_historical_station_metadata,
    get_historical_half_hourly_air_temperature_readings
)
from ..dagster_utils.constants import (
    DBT_PROJECT_DIR,
    DUCKDB_TARGET,
    DUCKDB_DIR,
    DUCKDB_SCHEMA
)
from ..dagster_utils.resources import (
    DuckDbConfig,
    DataGovAPI,
    AmazonS3
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
        "datagov_api": DataGovAPI(),
        "duckdb": DuckDBResource(
            database=DUCKDB_DIR
        ),
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR),
        "s3_client": AmazonS3(
            _aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
            _aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
            region_name=EnvVar("REGION")
        )
    }
)
