from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StringType, StructField, TimestampType
from pyspark.sql.functions import col, current_timestamp


def prepare_data(spark, crawl_time):
    print(crawl_time)
    df_job = (
        spark.read.option("header", True)
        .csv(f"hdfs://namenode:9000/user/root/job/job-{crawl_time}.csv")
    )

    df_job.show(n=20, truncate=True)
    df_job.printSchema()

    df_job_skill = spark.read.option("header", True).csv(
        f"hdfs://namenode:9000/user/root/job_skill/job_skill-{crawl_time}.csv"
    )

    df_job_skill.show(n=20, truncate=True)
    df_job_skill.printSchema()

    return df_job, df_job_skill


def insert_staging_data(spark, crawl_time):
    df, df_job_skill = prepare_data(spark, crawl_time)

    spark.sql("CREATE DATABASE IF NOT EXISTS staging;")
    spark.sql("DROP TABLE IF EXISTS staging.job_info;")
    spark.sql("DROP TABLE IF EXISTS staging.job_skill_info;")
    spark.sql(
        "CREATE TABLE IF NOT EXISTS staging.job_info (\
        id String, title String, company String, city String, \
        url String, created_date String) \
        USING hive;"
    )

    spark.sql(
        "CREATE TABLE IF NOT EXISTS staging.job_skill_info ( \
        id String, skill String) \
        USING hive;"
    )
    spark.sql("show databases").show(n=10)
    spark.sql("describe staging.job_info").show(n=50, truncate=True)
    spark.sql("describe staging.job_skill_info").show(n=50, truncate=True)
    df.write.mode("overwrite").format("hive").saveAsTable("staging.job_info")
    df_job_skill.write.mode("overwrite").format("hive").saveAsTable(
        "staging.job_skill_info"
    )
    spark.sql("SELECT * FROM staging.job_info").show(n=20)
    spark.sql("SELECT * FROM staging.job_skill_info").show(n=20)


def insert_public_data(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS public;")
    spark.sql("CREATE TABLE IF NOT EXISTS public.job_info ( \
        id String, title String, company String, city String, url String) \
        USING hive \
        PARTITIONED BY (created_date STRING);")
    spark.sql("MERGE INTO public.job_info USING (SELECT * FROM staging.job_info) sub ON staging.job_info.id = public.job_info.id \
        WHEN NOT MATCHED THEN INSERT VALUES (staging.job_info.id, staging.job_info.title, staging.job_info.company, staging.job_info.city, staging.job_info.url);")


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
    )

    spark = (
        SparkSession.builder.config(conf=conf)
        .enableHiveSupport()
        .getOrCreate()
    )
    insert_staging_data(spark, crawl_time)
    insert_public_data(spark)
    spark.sql("SELECT COUNT(DISTINCT id) FROM public.job_info").show(
        n=10, truncate=True)
    spark.sql("SELECT COUNT(id) FROM public.job_info").show(
        n=20, truncate=False)
    spark.sql("SELECT * FROM public.job_info").show(
        n=20, truncate=False)
