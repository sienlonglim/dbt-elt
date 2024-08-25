from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import hdb_dbt_asset
from ..dagster_utils.constants import DBT_PROJECT_DIR


defs = Definitions(
    assets=[hdb_dbt_asset],
    resources={
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR),
    }
)
