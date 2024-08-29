from dagster import job

from .ops import (
    op_check_S3_file_coverage,
    op_extract_and_upload
)


@job
def job_S3_hdb_resale_records_json():
    '''
    Retrieve hdb resale prices via API and upload to S3
    '''
    list_of_year_months_to_extract = op_check_S3_file_coverage()
    op_extract_and_upload(list_of_year_months_to_extract)

# df = pd.DataFrame(data['result']['records'])


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
