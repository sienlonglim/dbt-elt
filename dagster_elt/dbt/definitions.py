from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import build_all_assets
from ..dagster_utils.constants import DBT_PROJECT_DIR


defs = Definitions(
    assets=[build_all_assets],
    resources={
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR),
    }
)
