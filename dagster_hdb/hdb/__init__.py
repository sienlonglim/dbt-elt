from dagster import (
    AssetSelection,
    Definitions, 
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules
)
from . import assets, resources


all_assets = load_assets_from_modules([assets])

# Jobs
hdb_resale_transaction_job = define_asset_job("hdb_resale_transaction_job", selection=AssetSelection.all())

# Schedules
hdb_resale_transaction_schedule = ScheduleDefinition(
    job=hdb_resale_transaction_job,
    cron_schedule="* * * * *"
)

defs = Definitions(
    assets=all_assets,
    resources={
        "conn": resources.DataGovResourceAPI(user="my_user")
    },
    schedules=[hdb_resale_transaction_schedule]
)
