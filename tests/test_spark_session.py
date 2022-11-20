import unittest
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark import SparkConf

from airflow.dags.utils.spark_session import init_spark_session


class TestSparkSesison(unittest.TestCase):
    def test_init_spark_session(self):
        with patch('airflow.dags.utils.spark_session.SparkSession') as MockClass:
            instance = MockClass.return_value
            instance.SparkSession.return_value = None
            _ = init_spark_session()
