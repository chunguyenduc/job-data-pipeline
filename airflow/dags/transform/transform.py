import logging
from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from utils import queries, spark_session


def prepare_data(spark, crawl_time: str) -> Tuple[DataFrame, DataFrame]:
    logging.info(crawl_time)
    hdfs_base_path = "hdfs://namenode:9000/user/root/"
    df_job = (
        spark.read.option("header", True)
        .csv(hdfs_base_path + f"job/job-{crawl_time}.csv")
    )
    df_job = df_job.withColumn(
        "insert_time", F.current_timestamp().cast("timestamp"))
    df_job.show(n=20, truncate=True)
    df_job.printSchema()

    df_job_skill = spark.read.option("header", True).csv(
        hdfs_base_path + f"job_skill/job_skill-{crawl_time}.csv"
    )

    # Remove unnecessary tags
    blacklist_skills = ['Fresher Accepted']
    df_job_skill = df_job_skill.\
        filter(~df_job_skill.skill.isin(blacklist_skills)).withColumn(
            "insert_time", F.current_timestamp().cast("timestamp"))
    df_job_skill.show(n=20, truncate=True)
    df_job_skill.printSchema()

    return df_job, df_job_skill


def insert_staging_data(spark, crawl_time: str) -> None:
    df, df_job_skill = prepare_data(spark, crawl_time)

    spark.sql("CREATE DATABASE IF NOT EXISTS staging;")
    spark.sql("DROP TABLE IF EXISTS staging.job_info;")
    spark.sql("DROP TABLE IF EXISTS staging.job_skill_info;")

    spark.sql(queries.CREATE_STAGING_TABLE_JOB)
    spark.sql(queries.CREATE_STAGING_TABLE_JOB_SKILL)

    # insert to staging
    df.write.mode("overwrite").format("hive").saveAsTable("staging.job_info")
    df_job_skill.write.mode("overwrite").format(
        "hive").saveAsTable("staging.job_skill_info")

    # debugging
    logging.info("Show staging tables: ")
    spark.sql("SELECT * FROM staging.job_info").show(n=20)
    spark.sql("SELECT * FROM staging.job_skill_info").show(n=20)


def transform_insert_staging(crawl_time: str) -> None:
    spark = spark_session.init_spark_session()
    insert_staging_data(spark, crawl_time)
