import json
import re
from typing import Any

from dagster_aws.s3 import S3Resource
from dagster import (
    op,
    OpExecutionContext,
    EnvVar,
    get_dagster_logger,
    In,
    Nothing
)

from ..dagster_utils.resources import (
    DataGovAPI,
    MotherDuck,
    DuckDbConfig
)
from ..dagster_utils.ops import (
    op_list_S3_objects,
    op_upload_object_to_S3
)
from .config import (
    YEAR_MONTHS_TO_EXTRACT,
    S3_PREFIX
)


@op
def op_get_hdb_resale_records_json(
    datagov_api: DataGovAPI,
    year_month: str
) -> Any:
    '''
    Retrieve hdb resale prices and output as json object
    '''
    logger = get_dagster_logger()
    payload = {}
    filter_dict = {"month": [year_month]}
    payload["filters"] = json.dumps(filter_dict)
    payload["limit"] = 10000
    payload["sort"] = "month desc"
    response = datagov_api.request(
        resource_name="hdb_resale_prices",
        params=payload
    )
    logger.info(f"Get request sent: {response.url}")
    return response.json()


@op
def op_upload_hdb_resale_records_json_to_S3(
    s3_resource: S3Resource,
    data: Any,
    year_month: str
) -> None:
    logger = get_dagster_logger()
    filename = f"hdb_resale_records_{year_month}.json"
    data = json.dumps(data['result']['records'])
    op_upload_object_to_S3(
            s3_resource=s3_resource,
            file_object=data,
            bucket_name=EnvVar("AMAZON_S3_BUCKET_NAME").get_value(),
            key=S3_PREFIX,
            filename=filename
        )
    logger.info(f"Saved file as: {filename}")


@op
def op_check_S3_file_coverage(
    context: OpExecutionContext,
    s3_resource: S3Resource,
) -> list[str]:
    list_of_files_in_S3 = op_list_S3_objects(
        s3_resource=s3_resource,
        bucket_name=EnvVar("AMAZON_S3_BUCKET_NAME").get_value(),
        key=S3_PREFIX,
    )
    JSON_REGEX_PATTERN = re.compile(r'(\d{4}-\d{2})\.json$')
    year_months_in_S3 = [JSON_REGEX_PATTERN.search(filename).group(1) for filename in list_of_files_in_S3 if JSON_REGEX_PATTERN.search(filename)]
    list_of_year_months_to_extract = sorted(list(set(YEAR_MONTHS_TO_EXTRACT) - set(year_months_in_S3)))
    context.log.info(f"Files in S3: {len(year_months_in_S3)}, not in S3: {len(list_of_year_months_to_extract)}")
    return list_of_year_months_to_extract


@op
def op_extract_and_upload(
    context: OpExecutionContext,
    datagov_api: DataGovAPI,
    s3_resource: S3Resource,
    list_of_year_months_to_extract: list[str]
) -> None:
    context.log.info(f"Extracting {list_of_year_months_to_extract}")
    for year_month in list_of_year_months_to_extract:
        data = op_get_hdb_resale_records_json(
            datagov_api=datagov_api,
            year_month=year_month
        )
        op_upload_hdb_resale_records_json_to_S3(
            s3_resource=s3_resource,
            data=data,
            year_month=year_month
        )


@op
def op_initialize_db_and_table(
    context: OpExecutionContext,
    duckdb_config: DuckDbConfig,
    duckdb: MotherDuck
) -> None:
    with duckdb.get_connection() as conn:
        conn.query(f"create database if not exists {duckdb_config.database};")
        conn.query(f"create schema if not exists '{duckdb_config.database_schema}';")
        conn.query(f"""
            create table if not exists {duckdb_config.database}.{duckdb_config.database_schema}.raw__hdb_resale_records (
                _id text,
                month text,
                town text,
                flat_type text,
                block text,
                street_name text,
                storey_range text,
                floor_area_sqm text,
                flat_model text,
                lease_commence_date text,
                remaining_lease text,
                resale_price text,
                unique (_id, month)
            );
        """)
    context.log.info("Database and table initialized.")
    return


@op(ins={"ready": In(dagster_type=Nothing)})
def op_copy_into_motherduck(
    context: OpExecutionContext,
    duckdb_config: DuckDbConfig,
    duckdb: MotherDuck
) -> None:
    with duckdb.get_connection() as conn:
        conn.query(f"""
            insert or ignore into {duckdb_config.database}.{duckdb_config.database_schema}.raw__hdb_resale_records
            select *
            from 's3://{EnvVar("AMAZON_S3_BUCKET_NAME").get_value()}/{S3_PREFIX}/*.json'
        """)
    context.log.info("Insert into statement issued.")
