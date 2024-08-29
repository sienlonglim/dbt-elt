import os
from typing import Any

from dagster import (
    op,
    get_dagster_logger,
    OpExecutionContext
)
from dagster_aws.s3 import S3Resource

# from .resources import (
#     AmazonS3
# )


@op
def op_upload_object_to_S3(
    s3_resource: S3Resource,
    file_object: Any,
    bucket_name: str,
    key: str,
    filename: str
) -> None:
    logger = get_dagster_logger()
    prefix = os.path.join(key, filename).replace(os.path.sep, '/')
    logger.info(f"Uploading {filename} to {bucket_name}/{key}.")
    s3_resource.get_client().put_object(
        Bucket=bucket_name,
        Key=prefix,
        Body=file_object
    )


@op
def op_list_S3_objects(
    s3_resource: S3Resource,
    bucket_name: str,
    key: str
) -> list[str]:
    logger = get_dagster_logger()
    logger.info(f"Retrieving files in {bucket_name}/{key}.")
    response = s3_resource.get_client().list_objects_v2(
        Bucket=bucket_name,
        Prefix=key
    )
    return [entry['Key'] for entry in response.get('Contents')]


# @op
# def op_list_buckets(
#     s3_resource: AmazonS3
# ) -> None:
#     s3_resource.list_buckets()
