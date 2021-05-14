import pandas as pd
import psycopg2
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime
import logging

log = logging.getLogger(__name__)


class Load(BaseOperator):
    """
    Class to handle Extract and Load operations
    """

    @apply_defaults
    def __init__(self, county, db_conn, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.county = county
        self.connection = db_conn
    
    def execute(self, context):
        """
        Execute Task
        """
        # Retrieving the transformed data from Xcom variable
        load_data = context.get('ti').xcom_pull(key='extract_transform_data', task_ids='covid_data_extracttransform')

        # Retrieving data for the County of interest
        county_df = load_data[load_data['County']==self.county]
        county_df.drop(columns=['County'], inplace=True)
        county_df['Load Date'] = datetime.now().date()

        log.info('Started loading data to {} Table'.format(self.county))       
        result = county_df.to_sql(self.county, self.connection, index=False, if_exists='replace')
        
        if not result:
            log.info('Finished loading data to {} Table'.format(self.county))
        else:
            log.info('Failed loading data to database')
                