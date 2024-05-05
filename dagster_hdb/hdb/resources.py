from dagster import ConfigurableResource
# from dagster_dbt import (
#     DagsterDbtTranslator,
#     DbtCliResource,
#     dbt_assets,
#     get_asset_key_for_model,
# )
from .config import *
import requests

class DataGovResourceAPI(ConfigurableResource):
    user: str

    def request(
        self, 
        resource_id: str,
        params: dict
    ) -> requests.Response:
        
        return requests.get(
            f"https://data.gov.sg/api/action/datastore_search?resource_id={resource_id}",
            headers={"user-agent": "dagster"},
            params=params,
        )
    
# dbt_resource = DbtCliResource(project_dir=DBT_PROJECT_DIR)