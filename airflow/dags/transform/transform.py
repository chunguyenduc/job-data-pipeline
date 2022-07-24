from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, TimestampType
from pyspark.sql.functions import col, regexp_replace, trim, lag

spark = SparkSession.builder.appName("Transform").enableHiveSupport().getOrCreate()

schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("company", StringType(), False),
        StructField("city", StringType(), False),
        StructField("url", StringType(), False),
        StructField("created_at", TimestampType(), False),
    ]
)

df = (
    spark.read.option("header", True)
    .schema(schema)
    .csv("hdfs://namenode:9000/user/root/job/job-240722-2333.csv")
)
df.show(n=20, truncate=True)
df.printSchema()

df_job_skill = spark.read.option("header", True).csv(
    "hdfs://namenode:9000/user/root/job_skill/job_skill-240722-2333.csv"
)
df_job_skill.show(n=20, truncate=True)
df_job_skill.printSchema()
df_job_skill.groupBy("skill").count().show(n=20, truncate=False)

df_job_clean = df.select(
    # regexp_replace('title', r'[0-9]', ''), "title"
    trim(
        regexp_replace(
            "title",
            r"\([^()]*\)|\~|\$|([0-9])|\bUp.*\b|\- |\.|\bGấp[a-zA-Z]*|\.\bUrg.*\b|  +",
            "",
        )
    ).alias("title"),
    col("id"),
    col("city"),
    col("company"),
    col("created_at"),
)
df_job_clean.show(n=30, truncate=False)

spark.sql("CREATE DATABASE IF NOT EXISTS staging")
spark.sql(
    "CREATE TABLE IF NOT EXISTS staging.job_info (title String, id String, city String, company String, created_at TIMESTAMP) USING hive"
)
# df_job_clean.write.mode("append").format("hive").saveAsTable("staging.job_info")
spark.sql("select * from staging.job_info").show(n=50, truncate=False)
