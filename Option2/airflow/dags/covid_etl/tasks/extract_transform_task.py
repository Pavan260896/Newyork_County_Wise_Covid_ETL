import requests
import pandas as pd
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

log = logging.getLogger(__name__)


class ExtractTransform(BaseOperator):
    """
    Class to handle Extract and Load operations
    """

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def execute(self, context):
        """
        Execute Task
        """
        # Extracting raw data
        raw_data = requests.get('https://health.data.ny.gov/api/views/xdss-u53e/rows.json')
        data = raw_data.json()
        log.info('Extracted raw data')
        
        # Dropping unused variables
        df = pd.DataFrame(data['data'])
        df = df.drop(df.columns[0:8], axis=1)

        # Cleaning up and transforming raw data
        df.rename(columns={8: 'Test Date', 
                   9: 'County', 
                   10: 'New Positives', 
                   11: 'Cumulative Number of Positives', 
                   12: 'Total Number of Tests Performed', 
                   13:'Cumulative Number of Tests Performed'}, inplace=True)

        df['Test Date'] = pd.to_datetime(df['Test Date']).dt.date
        df['New Positives'] = df['New Positives'].astype(int)
        df['Cumulative Number of Positives'] = df['Cumulative Number of Positives'].astype(int)
        df['Total Number of Tests Performed'] = df['Total Number of Tests Performed'].astype(int)
        df['Cumulative Number of Tests Performed'] = df['Cumulative Number of Tests Performed'].astype(int)

        log.info('Transformed raw data')
        
        # Pushing the transformed data into Xcom variable for subsequent use
        context.get('ti').xcom_push(key='extract_transform_data', value = df)
        log.info('Loaded transformed data to context')
