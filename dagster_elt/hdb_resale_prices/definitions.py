from dagster import (
    Definitions
)

from .jobs import job_S3_hdb_resale_records_json
from ..dagster_utils.resources import (
    datagov_api_resource,
    duckdb_resource,
    dbt_resource,
    s3_resource
)


defs = Definitions(
    jobs=[job_S3_hdb_resale_records_json],
    resources={
        "datagov_api": datagov_api_resource,
        "duckdb": duckdb_resource,
        "dbt": dbt_resource,
        "s3_resource": s3_resource
    }
)
