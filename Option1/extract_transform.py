import requests
import pandas as pd 

class ExtractTransform():
    """
    Class to extract and transform the raw data
    """

    def __init__(self,url):
        self.url = url
        self.transformed_data = None
    
    def extract(self):
        response_data = requests.get(self.url)
        raw_data = response_data.json()
        self.transform(raw_data)
    
    def transform(self, raw_data):
        df = pd.DataFrame(raw_data['data'])
        df = df.drop(df.columns[0:8], axis=1)       # Dropping unrelevant data

        # Data clean up and transformation
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

        self.transformed_data = df

    

