import time

import pandas as pd
from datetime import datetime, timedelta
from dagster import (
    asset,
    get_dagster_logger,
    MetadataValue,
    MaterializeResult
)
from dagster_duckdb import DuckDBResource

from ..dagster_utils.resources import (
    DuckDbConfig,
    DataGovAPI,
)
from .config import (
    HISTORICAL_START_DATE,
    HISTORICAL_END_DATE
)


@asset(
    group_name="weather_air_temperatures",
    metadata={"dataset_name": "weather_air_temperatures"},
    compute_kind="duckdb"
)
def create_schema_and_table(
    duckdb_config: DuckDbConfig,
    duckdb: DuckDBResource
) -> None:
    '''
    Creates table for raw tables
    '''
    with duckdb.get_connection() as conn:
        conn.execute("create database if not exists 'raw';")
        conn.execute("set database 'raw';")
        conn.execute(f"create schema if not exists '{duckdb_config.database_schema}';")
        conn.execute(
            f"""
            create table if not exists {duckdb_config.database_schema}.air_temperatures (
                datetime DATETIME not null,
                station_id VARCHAR not null,
                value DECIMAL not null
            );
            """)
        conn.execute(
            f"""
            create table if not exists {duckdb_config.database_schema}.air_temperature_stations (
                id VARCHAR not null,
                device_id VARCHAR not null,
                name VARCHAR not null,
                location_latitude DECIMAL not null,
                location_longitude DECIMAL not null,
                last_date DATETIME not null
            );
            """)


@asset(
    deps=["create_schema_and_table"],
    group_name="weather_air_temperatures",
    metadata={"dataset_name": "weather_air_temperatures"},
    compute_kind="Request"
)
def get_historical_station_metadata(
    duckdb_config: DuckDbConfig,
    datagov_api: DataGovAPI,
    duckdb: DuckDBResource
) -> MaterializeResult:
    '''
    API call to retrieve all half hourly air temperature readings across weather stations
    '''
    logger = get_dagster_logger()
    start_date = datetime.strptime(HISTORICAL_START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(HISTORICAL_END_DATE, "%Y-%m-%d")
    curr_time = start_date
    df_list = []
    while (curr_time < end_date):
        formatted_timestr = curr_time.strftime("%Y-%m-%dT%H:%M:%S")
        payload = {'date_time': formatted_timestr}
        response = datagov_api.request(
            api_id="air_temperature",
            params=payload
        )
        logger.info(response.url)
        data = response.json()
        df_readings = pd.json_normalize(data['metadata']['stations'])
        df_readings["last_date"] = pd.Timestamp(formatted_timestr)
        df_list.append(df_readings)

        curr_time += timedelta(days=1)

    df = pd.concat(df_list)
    df = df.drop_duplicates(
        subset=df.columns[:-1],
        keep="last")
    df.columns = [col.replace(".", "_") for col in df.columns]

    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            create or replace table {duckdb_config.database_schema}.air_temperature_stations
            as (
                select * 
                from df
                union by name
                select *
                from {duckdb_config.database_schema}.air_temperature_stations 
            )
            """
        )

    return MaterializeResult(
        metadata={
            "timestamp": MetadataValue.text(str(datetime.now())),
            "len": MetadataValue.int(len(df))
        }
    )


@asset(
    deps=["create_schema_and_table"],
    group_name="weather_air_temperatures",
    metadata={"dataset_name": "weather_air_temperatures"},
    compute_kind="Request"
)
def get_historical_half_hourly_air_temperature_readings(
    duckdb_config: DuckDbConfig,
    datagov_api: DataGovAPI,
    duckdb: DuckDBResource
) -> MaterializeResult:
    '''
    API call to retrieve all half hourly air temperature readings across weather stations
    '''
    logger = get_dagster_logger()
    start_date = datetime.strptime(HISTORICAL_START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(HISTORICAL_END_DATE, "%Y-%m-%d")
    curr_time = start_date
    df_list = []
    call_count = 0
    while (curr_time < end_date):
        if call_count > 500:
            logger.info("Cooldown for 60s")
            time.sleep(60)
            call_count = 0
        formatted_timestr = curr_time.strftime("%Y-%m-%dT%H:%M:%S")
        payload = {'date_time': formatted_timestr}
        response = datagov_api.request(
            api_id="air_temperature", 
            params=payload
        )
        logger.info(response.url)
        data = response.json()
        df_readings = pd.DataFrame(data['items'][0]['readings'])
        timestamps = pd.Timestamp(formatted_timestr)
        df_readings.insert(
            loc=0,
            column="datetime",
            value=timestamps
        )
        df_list.append(df_readings)

        curr_time += timedelta(minutes=30)
        call_count += 1

    df = pd.concat(df_list)

    with duckdb.get_connection() as conn:
        conn.execute(
            f"""
            create or replace table {duckdb_config.database_schema}.air_temperatures
            as (
                select * 
                from df
                union by name
                select *
                from {duckdb_config.database_schema}.air_temperatures 
            )
            """
        )

    return MaterializeResult(
        metadata={
            "timestamp": MetadataValue.text(str(datetime.now())),
            "len": MetadataValue.int(len(df))
        }
    )
