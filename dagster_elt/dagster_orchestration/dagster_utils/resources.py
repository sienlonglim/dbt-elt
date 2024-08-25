import requests
import os
import sys
import logging
from typing import Any

import boto3
from dagster import (
    ConfigurableResource,
    EnvVar
)
from dagster_duckdb import DuckDBResource

from .constants import API_RESOURCE_MAPPER

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(message)s")
log = logging.getLogger()


class DbtDuckDbConfig(ConfigurableResource):
    DUCKDB_TARGET: str
    DBT_PROJECT_DIR: str
    DBT_MANIFEST_PATH: str
    DUCKDB_DIR: str
    DUCKDB_SCHEMA: str


class DataGovAPI(ConfigurableResource):
    """
    Resource for DataGovAPI
    """
    user: str

    def request(
        self,
        resource_name: str,
        params: dict = None
    ) -> requests.Response:
        response = requests.get(
            API_RESOURCE_MAPPER[resource_name],
            headers={"user-agent": "dagster"},
            params=params,
        )
        return response


class CustomDuckDBResource(DuckDBResource):
    user: str


class AmazonS3(ConfigurableResource):
    def __init__(
        self,
    ) -> None:
        self.client = boto3.client(
            's3',
            aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID").get_value(),
            aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY").get_value(),
            region_name=EnvVar("REGION").get_value()
        )
        log.info("Connected to S3")

    def list_buckets(
        self,
    ) -> None:
        response = self.client.list_buckets()
        for bucket in response['Buckets']:
            log.info(bucket["Name"])

    def upload_object(
        self,
        object: Any,
        bucket_name: str,
        path: str,
        filename: str
    ) -> None:
        keypath = os.path.join(path, filename).replace(os.path.sep, '/')
        self.client.put_object(
            Bucket=bucket_name,
            Key=keypath,
            Body=object
        )
        log.info(f"Uploaded {filename} successfully")
