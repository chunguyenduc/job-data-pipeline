import unittest
import unittest.mock as mock
from airflow.dags.utils.extract_helper import get_filename, get_id, write_data_to_csv
import pandas as pd


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


if __name__ == '__main__':
    unittest.main()
