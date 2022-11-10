import unittest
import unittest.mock as mock
from datetime import datetime

import pandas as pd
from airflow.dags.utils.extract_helper import (get_created_time,
                                               get_data_to_csv, get_filename,
                                               get_id, write_data_to_csv)


class TestExtractHelper(unittest.TestCase):
    def test_get_filename(self):
        prefix = "job"
        crawl_time = "221108"
        want = "/opt/airflow/dags/job-221108.csv"
        self.assertEqual(want, get_filename(crawl_time, prefix))

    def test_get_id(self):
        url = "https://itviec.com/it-jobs/business-analyst-product-owner-remote-boost-commerce-0245?lab_feature=preview_jd_page"
        want = "business-analyst-product-owner-remote-boost-commerce-0245"
        self.assertEqual(want, get_id(url))

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
        # temp = (pd.DataFrame(columns=JOB_FIELD, data=[["", "", "", "", "", [""]]]),
        #         pd.DataFrame(JOB_SKILL_FIELD))
        # pd.testing.assert_frame_equal(
        #     get_data_to_csv("", "", "", "", "", "", [""])[0], temp[0])
        pass


if __name__ == '__main__':
    unittest.main()
