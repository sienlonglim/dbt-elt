from dagster import (
    Definitions
)

from .assets import hdb_dbt_asset
from .jobs import (
    job_S3_hdb_resale_records_json,
    job_copyinto_duckdb
)
from ..dagster_utils.resources import (
    duckdb_config,
    datagov_api_resource,
    duckdb_resource,
    dbt_resource,
    s3_resource,
)


defs = Definitions(
    assets=[hdb_dbt_asset],
    jobs=[
        job_S3_hdb_resale_records_json,
        job_copyinto_duckdb
    ],
    resources={
        "duckdb_config": duckdb_config,
        "datagov_api": datagov_api_resource,
        "duckdb": duckdb_resource,
        "dbt": dbt_resource,
        "s3_resource": s3_resource
    }
)
