import unittest

from airflow.dags.utils.queries import CREATE_STAGING_TABLE_JOB


class TestQueries(unittest.TestCase):
    def test_queries(self):
        self.assertEqual(CREATE_STAGING_TABLE_JOB, "CREATE TABLE IF NOT EXISTS staging.job_info (\
        id String, title String, company String, city String, \
        url String, created_date String, insert_time Timestamp) \
        USING hive;")


if __name__ == '__main__':
    unittest.main()
