import json
import os
import pandas as pd
from datetime import datetime
from dagster import asset, AssetExecutionContext, MetadataValue, MaterializeResult
from ..resources import DataGovResourceAPI, CustomDuckDBResource
from .config import *

@asset(
    group_name = "hdb_resales",
    metadata = {"dataset_name": "hdb_resales"}
)
def get_hdb_resale_csv(
    context: AssetExecutionContext, 
    datagov_resource_conn: DataGovResourceAPI
) -> MaterializeResult:
    '''
    Retrieve hdb resale prices and save as a csv
    '''
    payload = {}
    filter_dict = {"month": [PREV_YEAR_MONTH]}
    payload["filters"] = json.dumps(filter_dict)
    payload["limit"] = 10000
    payload["sort"] = "month desc"
    

    response = datagov_resource_conn.request(
        resource_id="hdb_resale_prices",
        params=payload
    )
    data = response.json()

    os.makedirs("data", exist_ok=True)
    df = pd.DataFrame(data['result']['records'])
    df['date_ingested'] = pd.Timestamp("now")
    if len(df) > 0:
        df.to_csv(f"data/latest_hdb_resales_{PREV_YEAR_MONTH}.csv")
        context.log.info(f"CSV file saved.")
    else:
        context.log.info(f"Data is empty.")

    return MaterializeResult(
        metadata={
            "timestamp": MetadataValue.text(str(datetime.now())),
            "url":  MetadataValue.url(response.url),
            "num_records": MetadataValue.int(len(df)), 
            "preview": MetadataValue.md(df.head().to_markdown()) 
        }
    )


@asset(
    deps=[get_hdb_resale_csv],
    group_name = "hdb_resales",
    metadata = {"dataset_name": "hdb_resales"}
)
def create_schema_table(
    context: AssetExecutionContext, 
    duckdb: CustomDuckDBResource
) -> None:
    '''
    Create schema if it do not exists
    '''
    with duckdb.get_connection() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS RAW;")
        context.log.info("Create schema statement issued.")


@asset(
    deps=[create_schema_table],
    group_name = "hdb_resales",
    metadata = {"dataset_name": "hdb_resales"}
)
def resale_prices(
    context: AssetExecutionContext
) -> pd.DataFrame:
    '''
    Use DuckDB PandasIOManager to save pandas table directly into DuckDB as str types only
    '''
    df = pd.read_csv(
        f"data/latest_hdb_resales_{PREV_YEAR_MONTH}.csv",
        index_col=0,
        dtype=object
    )
    context.log.info(f"Reading csv file, total rows present: {len(df)}")