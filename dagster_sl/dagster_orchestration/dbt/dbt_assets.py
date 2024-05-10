from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets
from ..config import *

@dbt_assets(manifest=DBT_MANIFEST_PATH)
def hdb_asset(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()