import unittest
from extract_transform import ExtractTransform
import pandas as pd 


class TestExtractTransform(unittest.TestCase):
    """
    Test suit for determinig the validity of data
    """

    def setUp(self):
        start_extract_transform = ExtractTransform('https://health.data.ny.gov/api/views/xdss-u53e/rows.json')
        start_extract_transform.extract()
        self.test_data = start_extract_transform.transformed_data
    
    def test_data(self):

        # Testing for Null values in 'Test Date'
        result_test_date = any(self.test_data['Test Date'].isna())
        self.assertFalse(result_test_date)
        
        # Testing for non-negative values in 'New Positives'
        result_new_positives = all(self.test_data['New Positives'] >=0)
        self.assertTrue(result_new_positives)
        
        # Testing for non-negative values in 'Cumulative Number of Positives'
        result_cum_num_positives = all(self.test_data['Cumulative Number of Positives'] >=0)
        self.assertTrue(result_cum_num_positives)
        
        # Testing for non-negative values in 'Total Number of Tests Performed'
        result_total_num_tests_performed = all(self.test_data['Total Number of Tests Performed'] >=0)
        self.assertTrue(result_total_num_tests_performed)
        
        # Testing for non-negative values in 'Cumulative Number of Tests Performed
        result_cum_num_tests_performed = all(self.test_data['Cumulative Number of Tests Performed'] >=0)
        self.assertTrue(result_cum_num_tests_performed)


if __name__ == "__main__":
    unittest.main()