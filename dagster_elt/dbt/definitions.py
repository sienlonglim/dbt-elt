from dagster import Definitions

from .assets import build_all_assets
from ..dagster_utils.resources import dbt_resource


defs = Definitions(
    assets=[build_all_assets],
    resources={
        "dbt": dbt_resource,
    }
)
