import unittest
import unittest.mock as mock
from datetime import datetime

import pandas as pd
from airflow.dags.utils.extract_helper import get_created_time, get_filename, get_id, write_data_to_csv


class TestExtractHelper(unittest.TestCase):
    def test_get_filename(self):
        class Request:
            def __init__(self, name: str, prefix: str, crawl_time: str, format_type: str):
                self.name = name
                self.prefix = prefix
                self.crawl_time = crawl_time
                self.format_type = format_type

        requests = [
            Request(name="TC1", prefix="job",
                    crawl_time="221108", format_type="csv")
        ]
        expecteds = [
            "/opt/airflow/dags/job-221108.csv"
        ]
        for req, exp in zip(requests, expecteds):
            actual = get_filename(req.crawl_time, req.prefix, req.format_type)
            self.assertEqual(exp, actual)

    def test_get_id(self):
        class Request:
            def __init__(self, name: str, url: str):
                self.name = name
                self.url = url
        requests = [
            Request(
                name="TC1",
                url="https://itviec.com/it-jobs/business-analyst-product-owner-remote-boost-commerce-0245?lab_feature=preview_jd_page")]
        expecteds = [
            "business-analyst-product-owner-remote-boost-commerce-0245"
        ]
        for req, exp in zip(requests, expecteds):
            actual = get_id(req.url)
            self.assertEqual(exp, actual)

    def test_write_data_to_csv(self):
        # test_df = pd.DataFrame()
        # with mock.patch("pandas.DataFrame.to_csv") as to_csv_mock:
        #     write_data_to_csv(test_df, "221108", "job")
        #     to_csv_mock.assert_called_with(
        #         "/opt/airflow/dags/job-221108.csv", index=False)
        pass

    def test_get_created_time(self):
        distance_time_req = ["1m", "1h", "1d", "XXX"]
        time_now = datetime(2022, 1, 1, 7, 30, 0)
        expected = [datetime(2022, 1, 1, 7, 29, 0), datetime(
            2022, 1, 1, 6, 30, 0), datetime(2021, 12, 31, 7, 30, 0), time_now]

        for i, req in enumerate(distance_time_req):
            self.assertEqual(get_created_time(
                time_now, req), expected[i])


if __name__ == '__main__':
    unittest.main()
