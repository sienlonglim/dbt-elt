'''
This script performs the ETL process, config.yaml specifies which months' data to extract, transform, and load as a csv
1. API call is made to extract monthly data from data.gov.sg
2. Script performs tranformation of data, and searches for geolocations and routing to city center + nearest mrt stations
3. Splits data into train (all previous months) and test set (most recent month)
'''
import os
import requests
import requests_cache
import numpy as np
import pandas as pd
import json
import yaml
from datetime import datetime
from utils import *

# Logger here is the debugger logger
logger.info(f"{'-'*50}New run started {'-'*50}")

@error_handler
def get_location_data(address_df: pd.DataFrame, verbose : int=0, cached_session : requests_cache.CachedSession =None):
    '''
    Function to carry out API call for Geodata
    ## Parameters
    address_df : pd.DataFrame
        DataFrame that contains a combination of ['block'] and ['street_name'] as ['address'], and ['town']
    verbose : int
        1 to verbose calls
        2 to verbose results
    '''
    # Getting latitude, longitude, postal code
    def get_lat_long(address_df : pd.DataFrame, sleeptime : float =0.15):
        '''
        The actual API call to be called row-wise to get latitude, longitude, and postal code
        ## Parameters
        address_df : pd.DataFrame
            DataFrame that contains a combination of ['block'] and ['street_name'] as ['address'], and ['town']
        sleeptime : float
            Incorporates sleep time to not exceed a max of 250 calls per min
            Default 0.15s, not required if we are caching call
        '''
        
        # Lag time between calls - No longer needed with Cache, since we will not likely exceed the call limit
        # time.sleep(sleeptime)

        # API call
        try:
            address = address_df['address']
            if 'Jln Batu' in address:
                address = address.replace('Jln Batu', 'JALAN BATU DI TANJONG RHU')
            elif '27, Marine Cres' in address:
                address = address.replace('Marine Cres', 'MARINE CRESCENT MARINE CRESCENT VILLE')
            elif '215, Choa Chu Kang Ctrl' in address:
                address = '680215'
            elif '216, Choa Chu Kang Ctrl' in address:
                address = '680216'
                
            call = f"https://www.onemap.gov.sg/api/common/elastic/search?searchVal={address}&returnGeom=Y&getAddrDetails=Y"
            # Caching is enabled in the session
            response = cached_session.get(call)
            response.raise_for_status()
            data = response.json()
            if verbose >0:
                logger.info(f'Response: {response.status_code} \tGet request call: {response.url}')
            if verbose >1:
                logger.info(data)

            # Returns a the results in string
            return data['results'][0]['LATITUDE'] + ',' + data['results'][0]['LONGITUDE'] + ' ' + data['results'][0]['POSTAL']
        
        except Exception as err:
            logger.error(f'Error occurred - get_lat_long() API call: {err} on the following call:', exc_info=True)
            return '0,0 0' # Still return 0 values

    def to_numpy_array(lat_long_df):
        # Build a numpy array from latitude and longitude
        combi = np.array([lat_long_df[0], lat_long_df[1]])
        return combi
    
    # This calls the API call function row wise
    position = address_df.apply(get_lat_long, axis=1)

    try:
        # Split the string into two columns (column 0 is the latitude and longitude, column 1 is the postal code)
        temp_df = position.str.split(expand=True)
        # Postal code
        temp_df.iloc[:,1] = temp_df.iloc[:,1].apply(lambda x: 0 if x=='NIL' else x)
        temp_df.iloc[:,1] = temp_df.iloc[:,1].astype('int')
        # Latitude and longitude split (by ,)
        lat_long_df = temp_df.iloc[:,0].str.split(pat=',', expand=True)
        lat_long_df = lat_long_df.astype('float')
        # Convert into numpy array, for faster matrix operations later
        numpy_array = lat_long_df.apply(to_numpy_array, axis=1)
        
    except Exception as err:
        logger.error(f"Error occurred - Splitting data : {err}")
    else:
        geo_data_df = pd.concat([temp_df, lat_long_df, numpy_array], axis=1)
        geo_data_df.columns = ['lat_long', 'postal_code', 'latitude', 'longitude', 'numpy_array']
        return geo_data_df

if __name__ ==  '__main__':
    # Get the correct etl_logger
    etl_logger = add_custom_logger('etl', file_path='logs/etl.log')
    etl_logger.info(f"{'-'*50}New ETL run started {'-'*50}")

    # Enable caching
    session = requests_cache.CachedSession(cache_filepath, backend="sqlite")

    etl_logger.info('Getting geolocations')
    geo_data_df= get_location_data(df[['address']], verbose=1, cached_session=session)
    etl_logger.info('\t\tCompleted')

    