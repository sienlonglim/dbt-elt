from datetime import datetime

import pandas as pd
from dagster import (
    asset,
    get_dagster_logger,
    MetadataValue,
    MaterializeResult
)

from .ops import op_get_hdb_resale_records_json


@asset(
    group_name="hdb_resales",
    metadata={"dataset_name": "hdb_resales"}
)
def hdb_resale_S3_files() -> MaterializeResult:
    '''
    Retrieve hdb resale prices and save as a csv
    '''
    logger = get_dagster_logger()
    data = op_get_hdb_resale_records_json()
    df = pd.DataFrame(data['result']['records'])
    if len(df) < 1:
        logger.info("Data is empty.")

    return MaterializeResult(
        metadata={
            "timestamp": MetadataValue.text(str(datetime.now())),
            "num_records": MetadataValue.int(len(df)),
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )


# @asset(
#     deps=[get_hdb_resale_records_S3],
#     group_name="hdb_resales",
#     metadata={"dataset_name": "hdb_resales"}
# )
# def create_schema_table(
#     context: AssetExecutionContext, 
#     duckdb: DuckDBResource
# ) -> None:
#     '''
#     Create schema if it do not exists
#     '''
#     with duckdb.get_connection() as conn:
#         conn.execute("CREATE SCHEMA IF NOT EXISTS RAW;")
#         context.log.info("Create schema statement issued.")


# @asset(
#     deps=[create_schema_table],
#     group_name="hdb_resales",
#     metadata={"dataset_name": "hdb_resales"}
# )
# def resale_prices(
#     context: AssetExecutionContext
# ) -> pd.DataFrame:
#     '''
#     Use DuckDB PandasIOManager to save pandas table directly into DuckDB as str types only
#     '''
#     df = pd.read_csv(
#         f"data/latest_hdb_resales_{PREV_YEAR_MONTH}.csv",
#         index_col=0,
#         dtype=object
#     )
#     context.log.info(f"Reading csv file, total rows present: {len(df)}")
