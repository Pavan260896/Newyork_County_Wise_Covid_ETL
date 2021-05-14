import pandas as pd
import sqlalchemy as sa
from datetime import datetime

class LoadData():
    """
    Class to load individual County data into its corresponding table
    """

    def __init__(self, county, data_to_load, conn):
        self.county = county
        self.data_to_load = data_to_load
        self.conn = conn
    
    def load_data_to_table(self):
        self.data_to_load.drop(columns=['County'], inplace=True)
        self.data_to_load['Load Date'] = datetime.now().date()
        print(f'Started loading for {self.county}')
        self.data_to_load.to_sql(self.county, self.conn, index=False, if_exists='replace')
        print(f'Finished loading for {self.county}')
