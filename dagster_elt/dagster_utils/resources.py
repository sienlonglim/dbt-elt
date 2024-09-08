import requests
import os
import sys
import logging
from typing import Any, Optional
from contextlib import contextmanager

import boto3
import duckdb
from dagster import (
    ConfigurableResource,
    InitResourceContext,
    EnvVar
)
from dagster_dbt import DbtCliResource
from dagster_aws.s3 import S3Resource
from dagster_duckdb import DuckDBResource

from .constants import (
    API_RESOURCE_MAPPER,
    DBT_PROJECT_DIR,
    DUCKDB_DIR,
    LOCAL_DUCKDB
)


logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(message)s")
log = logging.getLogger()


class DuckDbConfig(ConfigurableResource):
    database: str
    database_schema: str
    directory: Optional[str] = None


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


class CustomAmazonS3(ConfigurableResource):
    _aws_access_key_id: str
    _aws_secret_access_key: str
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


class MotherDuck(ConfigurableResource):
    motherduck_token: str
    database: str

    @contextmanager
    def get_connection(self):
        conn = duckdb.connect(
            f"md:{self.database}?motherduck_token={self.motherduck_token}",
            config={'threads': 1}
        )

        yield conn

        conn.close()


# Create resource instances
datagov_api_resource = DataGovAPI()
dbt_resource = DbtCliResource(project_dir=DBT_PROJECT_DIR)
s3_resource = S3Resource(
    aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
    region_name=EnvVar("REGION")
)
if LOCAL_DUCKDB:
    duckdb_config = DuckDbConfig(
        database=EnvVar("DUCKDB_DATABASE"),
        database_schema=EnvVar("DUCKDB_SCHEMA"),
        directory=DUCKDB_DIR
    )
    duckdb_resource = DuckDBResource(
        database=duckdb_config.directory
    )
else:
    duckdb_config = DuckDbConfig(
        database=EnvVar("MOTHERDUCK_DATABASE"),
        database_schema=EnvVar("MOTHERDUCK_SCHEMA")
    )
    duckdb_resource = MotherDuck(
        motherduck_token=EnvVar("MOTHERDUCK_TOKEN"),
        database=duckdb_config.database
    )
