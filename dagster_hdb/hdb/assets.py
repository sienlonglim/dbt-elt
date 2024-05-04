import json
import os
import pandas as pd
from datetime import date, datetime, timedelta
from dagster import asset, AssetExecutionContext, MetadataValue, MaterializeResult
from .resources import DataGovResourceAPI

@asset(
    group_name = "hdb_resale_transactions",
    metadata = {"dataset_name": "HDB Resale Transactions"}
)
def previous_month_hdb_resale_transactions(
    context: AssetExecutionContext, 
    conn: DataGovResourceAPI
) -> MaterializeResult:
    '''
    Retrieve previous month's hdb resale prices
    '''
    payload = {}

    # Use current day to get previous month and year
    current_date = date.today()
    first_day = current_date.replace(day=1)
    last_month = first_day - timedelta(days=1)

    year_month = f'{str(last_month.year)}-{str(last_month.month).zfill(2)}'
    filter_dict = {"month": [year_month]}
    payload["filters"] = json.dumps(filter_dict)
    payload["limit"] = 10000
    payload["sort"] = "month desc"
    
    try:
        response = conn.request(
            "d_8b84c4ee58e3cfc0ece0d773c8ca6abc",
            params=payload
        )
        context.log.info(f"{response.status_code} - {response.url}")
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        context.log.error(f"{e}")

    os.makedirs("data", exist_ok=True)
    df = pd.DataFrame(data['result']['records'])
    if len(df) > 0:
        df.to_csv(f"data/latest_hdb_resales_{year_month}.csv")
        context.log.info(f"CSV file saved")
    else:
        context.log.info(f"Data is empty")

    return MaterializeResult(
        metadata={
            "timestamp": MetadataValue.text(str(datetime.now())),
            "url":  MetadataValue.url(response.url),
            "num_records": MetadataValue.int(len(df)), 
            "preview": MetadataValue.md(df.head().to_markdown()) 
        }
    )


# Testing purposes
if __name__ == "__main__":
    pass