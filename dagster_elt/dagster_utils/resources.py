import requests
import os
import sys
import logging
from typing import Any

import boto3
from dagster import (
    ConfigurableResource,
    InitResourceContext
)
from pydantic import PrivateAttr

from .constants import API_RESOURCE_MAPPER


logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(message)s")
log = logging.getLogger()


class DuckDbConfig(ConfigurableResource):
    target_database: str
    directory: str
    database_schema: str


class DataGovAPI(ConfigurableResource):
    """
    Resource for DataGovAPI
    """
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


class AmazonS3(ConfigurableResource):
    _aws_access_key_id: str = PrivateAttr()
    _aws_secret_access_key: str = PrivateAttr()
    region_name: str

    def setup_for_execution(
        self,
        context: InitResourceContext
    ) -> None:
        self.client = boto3.client(
            's3',
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            region_name=self.region_name
        )
        # context.info("Connected to S3")

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
