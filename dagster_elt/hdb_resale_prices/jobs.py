from dagster import job

from .ops import (
    op_check_S3_file_coverage,
    op_extract_and_upload,
    op_initialize_db_and_table,
    op_copy_into_motherduck
)


@job
def job_S3_hdb_resale_records_json():
    '''
    Retrieve hdb resale prices via API and upload to S3
    '''
    list_of_year_months_to_extract = op_check_S3_file_coverage()
    op_extract_and_upload(list_of_year_months_to_extract)


@job
def job_copyinto_duckdb():
    '''
    Copy S3 json data into MotherDuck
    '''
    ready = op_initialize_db_and_table()
    op_copy_into_motherduck(ready)
