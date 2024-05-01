import json
import os
import requests
import pandas as pd
from datetime import date, datetime
from dagster import asset 

@asset
def latest_hdb_resales() -> None:
    '''
    API call to retrieve current month's hdb resale prices
    '''
    data_gov_url = "https://data.gov.sg/api/action/datastore_search?resource_id=d_8b84c4ee58e3cfc0ece0d773c8ca6abc"
    payload = {}
    current_date = date.today()
    year_month = f'{str(current_date.year)}-{str(current_date.month).zfill(2)}'
    filter_dict = {"month": [year_month]}
    payload["filters"] = json.dumps(filter_dict)
    payload["limit"] = 10000
    payload["sort"] = "month desc"
    
    print(payload)
    response = requests.get(data_gov_url, params=payload)
    response.raise_for_status()
    data = response.json()

    os.makedirs("data", exist_ok=True)
    df = pd.DataFrame(data['result']['records'])
    df.to_csv(f"data/latest_hdb_resales_{current_date}.csv")

@asset
def latest_air_temperature_readings() -> None:
    '''
    API call to retrieve air temperature readings across weather stations
    '''
    current_time = datetime.now()
    formatted_timestr = current_time.strftime("%Y-%m-%dT%H:%M:%S")
    air_temperature_url = "https://api.data.gov.sg/v1/environment/air-temperature"
    payload = {'date_time': formatted_timestr}
    response = requests.get(air_temperature_url, params=payload)
    response.raise_for_status()
    data = response.json()
    os.makedirs("data", exist_ok=True)
    with open("data/air_temperature_readings.json", "w") as f:
        json.dump(data, f)

# Testing purposes
if __name__ == "__main__":
    pass