from dagster import AssetExecutionContext
from dagster_dbt import (
    DbtCliResource,
    dbt_assets
)
from ..dagster_utils.constants import DBT_MANIFEST_PATH


@dbt_assets(manifest=DBT_MANIFEST_PATH)
def hdb_dbt_asset(
    context: AssetExecutionContext,
    dbt: DbtCliResource
):
    yield from dbt.cli(["build", "--select", "stg__hdb_resale_records"], context=context).stream()
