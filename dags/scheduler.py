from datetime import datetime, timedelta
from airflow import DAG

from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.hive_operator import HiveOperator
# from airflow.operators.
from airflow.operators.python import PythonOperator 