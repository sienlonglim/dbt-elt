import json
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dagster import asset, AssetExecutionContext, MetadataValue, MaterializeResult
from .config import *

@asset(
    group_name = "weather_temperature",
    metadata = {"dataset_name": "weather_temperature"}
)
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

@asset(
    group_name = "weather_temperature",
    metadata = {"dataset_name": "weather_temperature"}
)
def historical_half_hourly_air_temperature_readings(context: AssetExecutionContext) -> MaterializeResult:
    '''
    API call to retrieve all half hourly air temperature readings across weather stations
    '''
    start_date = datetime.strptime(HISTORICAL_START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(HISTORICAL_END_DATE, "%Y-%m-%d")
    curr_time = start_date
    df_list = []
    while (curr_time <= end_date):
        formatted_timestr = curr_time.strftime("%Y-%m-%dT%H:%M:%S")
        air_temperature_url = "https://api.data.gov.sg/v1/environment/air-temperature"
        payload = {'date_time': formatted_timestr}
        try:
            response = requests.get(air_temperature_url, params=payload)
            response.raise_for_status()
            data = response.json()
            df_readings = pd.DataFrame(data['items'][0]['readings'])
            df_readings['datetime'] = pd.Timestamp(formatted_timestr)
            df_readings = df_readings.set_index('datetime')
            df_list.append(df_readings)
        except Exception as e:
            context.log.error(f"{e}")
        curr_time += timedelta(minutes=30)

    os.makedirs("data", exist_ok=True)
    df = pd.concat(df_list)
    df.to_csv(f"data/half_hourly_air_temp_{HISTORICAL_START_DATE}_{HISTORICAL_END_DATE}.csv")
    context.log.info(f"CSV file saved")

    return MaterializeResult(
        metadata={
            "timestamp": MetadataValue.text(str(datetime.now()))
        }
    )