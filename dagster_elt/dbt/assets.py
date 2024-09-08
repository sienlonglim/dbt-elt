from dagster import AssetExecutionContext
from dagster_dbt import (
    DbtCliResource,
    dbt_assets
)
from ..dagster_utils.constants import DBT_MANIFEST_PATH


@dbt_assets(manifest=DBT_MANIFEST_PATH)
def build_all_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource
):
    yield from dbt.cli(["build"], context=context).stream()
