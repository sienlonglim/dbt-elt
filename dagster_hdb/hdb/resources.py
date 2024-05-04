from dagster import ConfigurableResource
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