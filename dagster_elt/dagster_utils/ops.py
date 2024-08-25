from typing import Any

from dagster import (
    op,
    get_dagster_logger,
)


@op
def upload_to_S3(
    object: Any,
    
):
    log = get_dagster_logger()
    pass
