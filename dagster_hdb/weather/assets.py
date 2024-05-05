import json
import os
import requests
import pandas as pd
from datetime import datetime
from dagster import asset, AssetExecutionContext, MetadataValue, MaterializeResult

@asset
def latest_air_temperature_readings(context: AssetExecutionContext) -> MaterializeResult:
    '''
    API call to retrieve air temperature readings across weather stations
    '''
    current_time = datetime.now()
    formatted_timestr = current_time.strftime("%Y-%m-%dT%H:%M:%S")
    air_temperature_url = "https://api.data.gov.sg/v1/environment/air-temperature"
    payload = {'date_time': formatted_timestr}

    try:
        response = requests.get(air_temperature_url, params=payload)
        context.log.info(f"{response.status_code} - {response.url}")
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        context.log.error(f"{e}")

    os.makedirs("data", exist_ok=True)
    with open("data/air_temperature_readings.json", "w") as f:
        json.dump(data, f)
    context.log.info(f"JSON file saved")

    return MaterializeResult(
        metadata={
            "timestamp": MetadataValue.text(str(datetime.now())),
            "url":  MetadataValue.url(response.url)
        }
    )

# Testing purposes
if __name__ == "__main__":
    pass