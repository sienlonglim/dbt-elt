import json

from dagster import (
    op,
    get_dagster_logger
)

from ..dagster_utils.resources import (
    DataGovAPI
)
from .config import PREV_YEAR_MONTH


@op
def op_get_hdb_resale_records_json(
    datagov_api: DataGovAPI
):
    '''
    Retrieve hdb resale prices and output as json object
    '''
    log = get_dagster_logger()
    payload = {}
    filter_dict = {"month": [PREV_YEAR_MONTH]}
    payload["filters"] = json.dumps(filter_dict)
    payload["limit"] = 10000
    payload["sort"] = "month desc"
    response = datagov_api.request(
        resource_id="hdb_resale_prices",
        params=payload
    )
    log.info(f"Get request sent: {response.url}")
    return response.json()
