from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StringType, StructField, TimestampType
from pyspark.sql.functions import col, current_timestamp

master = "spark://spark:7077"
conf = (
    SparkConf()
    .setAppName("Transform")
    .setMaster(master)
    .set("spark.memory.offHeap.enabled", True)
    .set("spark.memory.offHeap.size", "10g")
    .set("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .set("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
)

sc = SparkContext(conf=conf)
spark = (
    SparkSession.builder.config(conf=conf)
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "10g")
    .enableHiveSupport()
    .config("hive.exec.dynamic.partition","true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .getOrCreate()
)


def prepare_data(crawl_time):
    print(crawl_time)
    df = (
        spark.read.option("header", True)
        .csv(f"hdfs://namenode:9000/user/root/job/job-{crawl_time}.csv")
    )
    df_job = df.select(
        col("*"),
        current_timestamp().alias("insert_at")
    )
    df_job.show(n=20, truncate=True)
    df_job.printSchema()

    df_job_skill = spark.read.option("header", True).csv(
        f"hdfs://namenode:9000/user/root/job_skill/job_skill-{crawl_time}.csv"
    )
    df_job_skill = df_job_skill.select(
        col("*"),
        current_timestamp().alias("insert_at")
    )
    df_job_skill.show(n=20, truncate=True)
    df_job_skill.printSchema()

    return df_job, df_job_skill


def insert_staging_data(crawl_time):
    df, df_job_skill = prepare_data(crawl_time)

    spark.sql("CREATE DATABASE IF NOT EXISTS staging;")
    spark.sql("DROP TABLE IF EXISTS staging.job_info;")
    spark.sql("DROP TABLE IF EXISTS staging.job_skill_info;")
    spark.sql(
        "CREATE TABLE IF NOT EXISTS staging.job_info (\
        id String, title String, company String, city String, \
        url String, created_date String, insert_at Timestamp) \
        USING hive;"
    )

    spark.sql(
        "CREATE TABLE IF NOT EXISTS staging.job_skill_info ( \
        id String, skill String, insert_at Timestamp) \
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

def insert_public_data():
    spark.sql("CREATE DATABASE IF NOT EXISTS public;")
    spark.sql("CREATE TABLE IF NOT EXISTS public.job_info ( \
        id String, title String, company String, city String, url String, insert_at Timestamp) \
        USING hive \
        PARTITIONED BY (created_date STRING);")
    spark.sql("INSERT INTO TABLE public.job_info PARTITION(created_date) \
    SELECT id, title, company, city, url, insert_at, created_date \
    FROM staging.job_info")

def load_data(crawl_time):
    insert_staging_data(crawl_time)
    insert_public_data()
    spark.sql("SELECT COUNT(DISTINCT id) FROM public.job_info").show(n=10, truncate=True)
    spark.sql("SELECT * FROM public.job_info WHERE id = 3208").show(n=20, truncate=False)

    
