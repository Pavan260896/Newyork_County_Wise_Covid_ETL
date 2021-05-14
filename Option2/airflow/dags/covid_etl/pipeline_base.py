from airflow import DAG
import datetime
from datetime import timedelta
from covid_etl.tasks.extract_transform_task import ExtractTransform
from covid_etl.tasks.load_task import Load
import pandas as pd
import sqlalchemy as sa
from sqlalchemy_utils import database_exists, create_database

# set default arguments
default_args = {
    'owner': 'Pavan',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 5, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Defining the DAG
etl_pipeline = DAG(
    'covid_data_loading', default_args=default_args,
    schedule_interval = "0 9 * * *", catchup=False)

# Creating a task for Extracting and Transforming raw data
extract_transform = ExtractTransform(task_id='covid_data_extracttransform',dag=etl_pipeline, provide_context=True)

# Creating database engine
counties = ['Albany', 'Allegany', 'Bronx', 'Broome', 'Cattaraugus', 'Cayuga', 'Chautauqua', 'Chemung', 'Chenango', 'Clinton', 'Columbia', 'Cortland', 'Delaware', 'Dutchess', 'Erie', 'Essex', 'Franklin', 'Fulton', 'Genesee', 'Greene', 'Hamilton', 'Herkimer', 'Jefferson', 'Kings', 'Lewis', 'Livingston', 'Madison', 'Monroe', 'Montgomery', 'Nassau', 'New York', 'Niagara', 'Oneida', 'Onondaga', 'Ontario', 'Orange', 'Orleans', 'Oswego', 'Otsego', 'Putnam', 'Queens', 'Rensselaer', 'Richmond', 'Rockland', 'Saratoga', 'Schenectady', 'Schoharie', 'Schuyler', 'Seneca', 'St. Lawrence', 'Steuben', 'Suffolk', 'Sullivan', 'Tioga', 'Tompkins', 'Ulster', 'Warren', 'Washington', 'Wayne', 'Westchester', 'Wyoming', 'Yates']
db_conn = sa.create_engine('postgresql://postgres:1234@postgres:5432/covid_data',
            execution_options={
        "isolation_level": "AUTOCOMMIT"
    })

# Creating 'covid_data' database if it doesn't exist
if not database_exists(db_conn.url):
    create_database(db_conn.url)

# Creating taks to load data into Postgres database 
task_list = []
for county in counties:
    county_name = county.replace(' ', '_')
    task_id = f'covid_data_load_{county_name}'
    a = Load(county=county, db_conn=db_conn, task_id=task_id, dag=etl_pipeline, provide_context=True)
    task_list.append(a)

# Defining task order
extract_transform >> task_list