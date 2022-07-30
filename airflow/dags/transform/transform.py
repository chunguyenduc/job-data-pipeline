from pyspark import SparkConf, SparkContext, HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, TimestampType
from pyspark.sql.functions import col, regexp_replace, trim, lag

master = "spark://spark:7077"
conf = SparkConf().setAppName("Transform").setMaster(master). \
    set("spark.memory.offHeap.enabled", True).\
    set("spark.memory.offHeap.size", "10g").\
    set("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083").\
    set("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")

sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).config("spark.memory.offHeap.enabled","true")\
    .config("spark.memory.offHeap.size","10g").enableHiveSupport().getOrCreate()

def prepare_data(crawl_time):

    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("title", StringType(), False),
            StructField("company", StringType(), False),
            StructField("city", StringType(), False),
            StructField("url", StringType(), False),
            StructField("created_date", StringType(), False),
        ]
    )


    df = (
        spark.read.option("header", True)
        .schema(schema)
        .csv(f"hdfs://namenode:9000/user/root/job/job-{crawl_time}.csv")
    )
    df.show(n=20, truncate=True)
    df.printSchema()

    df_job_skill = spark.read.option("header", True).csv(
        f"hdfs://namenode:9000/user/root/job_skill/job_skill-{crawl_time}.csv"
    )
    df_job_skill.show(n=20, truncate=True)
    df_job_skill.printSchema()

    return df, df_job_skill

def insert_staging_data(crawl_time):
    df, df_job_skill = prepare_data(crawl_time)

    spark.sql("CREATE DATABASE IF NOT EXISTS staging;")
    spark.sql("DROP TABLE IF EXISTS staging.job_info;")
    spark.sql("DROP TABLE IF EXISTS staging.job__skill_info;")
    # with  open("/opt/airflow/dags/transform/create_staging_table.sql") as fr:
    #     query = fr.read()
    #     print(query)
    spark.sql("CREATE TABLE IF NOT EXISTS staging.job_info (\
        id String, title String, company String, city String, url String, created_date String) \
        USING hive;")

    spark.sql("CREATE TABLE IF NOT EXISTS staging.job_skill_info ( \
        id String, skill String) \
        USING hive;")
    spark.sql("show databases").show(n=10)
    spark.sql("describe staging.job_info").show(n=50, truncate=True)
    spark.sql("describe staging.job_skill_info").show(n=50, truncate=True)
    df.write.mode("overwrite").format("hive").saveAsTable("staging.job_info")
    df_job_skill.write.mode("overwrite").format("hive").saveAsTable("staging.job_skill_info")
    spark.sql("SELECT * FROM staging.job_info").show(n=20)
    spark.sql("SELECT * FROM staging.job_skill_info").show(n=20)






