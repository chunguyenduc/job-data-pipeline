import logging
from utils import queries, spark_session


def load_data() -> None:

    spark = spark_session.init_spark_session()
    spark.sql("CREATE DATABASE IF NOT EXISTS public;")

    spark.sql(queries.CREATE_PUBLIC_TABLE_JOB)
    spark.sql(queries.INSERT_PUBLIC_TABLE_JOB)

    spark.sql(queries.CREATE_PUBLIC_TABLE_JOB_SKILL)
    spark.sql(queries.INSERT_PUBLIC_TABLE_JOB_SKILL)

    logging.info("Showing public table: ")
