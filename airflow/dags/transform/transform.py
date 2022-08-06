from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
from transform import queries


def prepare_data(spark, crawl_time):
    logging.info(crawl_time)
    df_job = (
        spark.read.option("header", True)
        .csv(f"hdfs://namenode:9000/user/root/job/job-{crawl_time}.csv")
    )
    df_job = df_job.withColumn(
        "insert_time", F.current_timestamp().cast("timestamp"))
    df_job.show(n=20, truncate=True)
    df_job.printSchema()

    df_job_skill = spark.read.option("header", True).csv(
        f"hdfs://namenode:9000/user/root/job_skill/job_skill-{crawl_time}.csv"
    )

    blacklist_skills = ['Fresher Accepted']
    df_job_skill = df_job_skill.filter(~df_job_skill.skill.isin(blacklist_skills)).withColumn(
        "insert_time", F.current_timestamp().cast("timestamp"))
    df_job_skill.show(n=20, truncate=True)
    df_job_skill.printSchema()

    return df_job, df_job_skill


def insert_staging_data(spark, crawl_time):
    df, df_job_skill = prepare_data(spark, crawl_time)

    spark.sql("CREATE DATABASE IF NOT EXISTS staging;")
    spark.sql("DROP TABLE IF EXISTS staging.job_info;")
    spark.sql("DROP TABLE IF EXISTS staging.job_skill_info;")
    spark.sql(queries.CREATE_STAGING_TABLE_JOB)

    spark.sql(queries.CREATE_STAGING_TABLE_JOB_SKILL)

    # debugging
    spark.sql("show databases").show(n=10)
    spark.sql("describe staging.job_info").show(n=50, truncate=True)
    spark.sql("describe staging.job_skill_info").show(n=50, truncate=True)

    # insert to staging
    df.write.mode("overwrite").format("hive").saveAsTable("staging.job_info")
    df_job_skill.write.mode("overwrite").format(
        "hive").saveAsTable("staging.job_skill_info")

    # debugging
    spark.sql("SELECT * FROM staging.job_info").show(n=20)
    spark.sql("SELECT * FROM staging.job_skill_info").show(n=20)


def insert_public_data(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS public;")

    spark.sql(queries.CREATE_PUBLIC_TABLE_JOB)
    spark.sql(queries.INSERT_PUBLIC_TABLE_JOB)

    spark.sql(queries.CREATE_PUBLIC_TABLE_JOB_SKILL)
    spark.sql(queries.INSERT_PUBLIC_TABLE_JOB_SKILL)


def load_data(crawl_time):
    master = "spark://spark:7077"

    conf = (
        SparkConf()
        .setAppName("Transform")
        .setMaster(master)
        .set("spark.memory.offHeap.enabled", True)
        .set("spark.memory.offHeap.size", "10g")
        .set("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .set("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "10g")
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.session.timeZone", "UTC+7")
    )

    spark = (
        SparkSession.builder.config(conf=conf)
        .enableHiveSupport()
        .getOrCreate()
    )
    insert_staging_data(spark, crawl_time)
    insert_public_data(spark)

    logging.info("Showing hive table")
    # debugging
    spark.sql("SELECT COUNT(DISTINCT id) FROM public.job_info").show(
        n=10, truncate=True)
    spark.sql("SELECT COUNT(id) FROM public.job_info;").show(
        n=20, truncate=False)
    spark.sql("SELECT * FROM public.job_info ORDER BY insert_time DESC;").show(
        n=20, truncate=False)
    spark.sql("SELECT id, url FROM public.job_info where id = (SELECT id FROM public.job_info GROUP BY id HAVING COUNT(id) > 1)").show(
        n=20, truncate=False)

    spark.sql("SELECT COUNT(DISTINCT id) FROM public.job_skill_info").show(
        n=10, truncate=True)
    spark.sql("SELECT COUNT(id) FROM public.job_skill_info").show(
        n=20, truncate=False)
    spark.sql("SELECT * FROM public.job_skill_info ORDER BY insert_time DESC;").show(
        n=20, truncate=False)
