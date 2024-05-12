from dagster import ConfigurableResource
from dagster_duckdb import DuckDBResource
import requests

class CustomConfig(ConfigurableResource):
    TARGET: str 
    DBT_PROJECT_DIR: str 
    DBT_MANIFEST_PATH: str 
    DUCKDB_DIR: str 
    SCHEMA: str 

class DataGovResourceAPI(ConfigurableResource):
    """
    Resource for DataGovAPI datastore search
    """
    user: str

    def request(
        self, 
        resource_id: str,
        params: dict=None
    ) -> requests.Response:
        
        api_id_dict= {
            "hdb_resale_prices": "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"
        }
        response = requests.get(
            f"https://data.gov.sg/api/action/datastore_search?resource_id={api_id_dict.get(resource_id)}",
            headers={"user-agent": "dagster"},
            params=params,
        )
        return response

class DataGovLiveAPI(ConfigurableResource):
    """
    Resource for DataGovAPI Live API search
    """
    user: str

    def request(
        self, 
        api_id: str,
        params: dict=None
    ) -> requests.Response:
        
        api_id_dict= {
            "air_temperature": "environment/air-temperature"
        }
        response = requests.get(
            f"https://api.data.gov.sg/v1/{api_id_dict.get(api_id)}",
            headers={"user-agent": "dagster"},
            params=params,
        )

        return response

class CustomDuckDBResource(DuckDBResource):
    user: str