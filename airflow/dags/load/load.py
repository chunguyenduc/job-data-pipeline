import logging
from utils import queries, spark_session


def load_data():

    spark = spark_session.init_spark_session()
    spark.sql("CREATE DATABASE IF NOT EXISTS public;")

    spark.sql(queries.CREATE_PUBLIC_TABLE_JOB)
    spark.sql(queries.INSERT_PUBLIC_TABLE_JOB)

    spark.sql(queries.CREATE_PUBLIC_TABLE_JOB_SKILL)
    spark.sql(queries.INSERT_PUBLIC_TABLE_JOB_SKILL)

    logging.info("Showing public table: ")
    # debugging
    spark.sql("SELECT COUNT(id), COUNT(DISTINCT id) FROM public.job_info").show(
        n=1, truncate=True)
    spark.sql("SELECT * FROM public.job_info ORDER BY insert_time DESC;").show(
        n=20, truncate=False)

    spark.sql("SELECT COUNT(id), COUNT(DISTINCT id) FROM public.job_skill_info").show(
        n=10, truncate=True)
    spark.sql("SELECT * FROM public.job_skill_info ORDER BY insert_time DESC;").show(
        n=20, truncate=False)
