from dagster import (
    Definitions,
    EnvVar
)
from dagster_dbt import DbtCliResource
from dagster_duckdb import DuckDBResource

from .assets import hdb_resale_S3_files
from ..dagster_utils.constants import (
    DBT_PROJECT_DIR,
    DUCKDB_DIR
)
from ..dagster_utils.resources import (
    DataGovAPI,
    AmazonS3
)


defs = Definitions(
    assets=[hdb_resale_S3_files],
    resources={
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
