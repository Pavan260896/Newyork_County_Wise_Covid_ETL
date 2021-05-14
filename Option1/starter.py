from extract_transform import ExtractTransform
from multi_tasking import load_thread_function
import pandas as pd 
import sqlalchemy as sa
import concurrent.futures

def startEtlProcess():
    """
    Function to start and supervise the ETL process.

    1. It calls on ExtractTransform which has extract and transform methods to retrieve and 
    then transform the raw data to load into the database
    2. It then creates a thread for each county and the threads load the data into the database
    """
    
    start_extract_transform = ExtractTransform('https://health.data.ny.gov/api/views/xdss-u53e/rows.json')
    start_extract_transform.extract()                           # Extracting the raw data
    finished_data = start_extract_transform.transformed_data    # Retrieving transformed data
    engine = sa.create_engine('sqlite:///covid_data.db', connect_args={'check_same_thread': False}, echo=False)     # Creating database engine
    counties = list(pd.unique(finished_data['County']))         # Getting the list of counties
    no_workers = len(counties) + 2
    load_list = []
    for county in counties:
        param_dict_loading = {}
        county_df = finished_data[finished_data['County']==county]
        param_dict_loading['county']=county
        param_dict_loading['data_to_load'] = county_df
        param_dict_loading['engine'] = engine
        load_list.append(param_dict_loading)                    # Defining parameters for each thread and adding them to a list
    with concurrent.futures.ThreadPoolExecutor(max_workers=no_workers) as executor:
        executor.map(load_thread_function,load_list)            # Mapping each thread to the load method along with the parameters

if __name__ == "__main__":

    print('Starting ETL Process')
    startEtlProcess()
    print('Ending ETL Process')