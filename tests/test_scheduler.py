import unittest

from airflow.dags.scheduler import *
from unittest.mock import patch


class TestScheduler(unittest.TestCase):
    @patch('airflow.dags.extract.job_spider')
    def test_scheduler(self, mock_extract):
        mock_extract.crawl_data.return_value = None
        # with self.assertRaises(Timeout):
        #     get_holidays()
        #     mock_requests.get.assert_called_once()
