import unittest
import unittest.mock as mock
from datetime import datetime
from typing import List

import pandas as pd
from airflow.dags.utils.extract_helper import (JOB_FIELD, JOB_SKILL_FIELD,
                                               get_created_time,
                                               get_data_to_csv, get_filename,
                                               get_id, write_data_to_csv, write_data_to_json)


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
        test_df = pd.DataFrame()
        with mock.patch("pandas.DataFrame.to_csv") as to_csv_mock:
            write_data_to_csv(test_df, "221108", "job")
            to_csv_mock.assert_called_with(
                "/opt/airflow/dags/job-221108.csv", index=False)

    def test_get_created_time(self):
        distance_time_req = ["1m", "1h", "1d", "XXX"]
        time_now = datetime(2022, 1, 1, 7, 30, 0)
        expected = [datetime(2022, 1, 1, 7, 29, 0), datetime(
            2022, 1, 1, 6, 30, 0), datetime(2021, 12, 31, 7, 30, 0), time_now]

        for i, req in enumerate(distance_time_req):
            self.assertEqual(get_created_time(
                time_now, req), expected[i])

    def test_get_data_to_csv(self):
        class Request:
            def __init__(self, name: str, job_id: str,
                         title: str,
                         company: str,
                         city: str,
                         url: str,
                         created_date: str,
                         skills: List[str]):
                self.name = name
                self.job_id = job_id
                self.title = title
                self.company = company
                self.city = city
                self.url = url
                self.created_date = created_date
                self.skills = skills

        requests = [
            Request("TC1", "id", "title", "company", "city",
                    "url", "created_date", ["python"])
        ]
        expecteds = [pd.DataFrame(columns=JOB_FIELD,
                                  data=[["id",
                                         "title",
                                        "company",
                                         "city",
                                         "url",
                                         ["python"],
                                         "created_date"]])]
        for req, expected in zip(requests, expecteds):
            actual = get_data_to_csv(
                req.job_id,
                req.title,
                req.company,
                req.city,
                req.url,
                req.created_date,
                req.skills)
            pd.testing.assert_frame_equal(
                actual, expected)


if __name__ == '__main__':
    unittest.main()
