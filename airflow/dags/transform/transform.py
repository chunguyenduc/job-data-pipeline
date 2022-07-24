from pyspark.sql import SparkSession
from  pyspark.sql.types import StructType, StringType, StructField, TimestampType
from pyspark.sql.functions import col, regexp_replace, trim, lag

spark = SparkSession.builder.appName("Transform").getOrCreate()

schema = StructType([
  StructField("id", StringType(), False),
  StructField("title", StringType(), False),
  StructField("company", StringType(), False),
  StructField("city", StringType(), False),
  StructField("url", StringType(), False),
  StructField("created_at", TimestampType(), False),
])

df = spark.read.\
    option("header", True).\
    schema(schema).\
    csv(
    "hdfs://namenode:9000/user/root/job/job-240722-1822.csv"
)
df.show(n=20, truncate=True)
df.printSchema()

df_job_skill = spark.read.option("header", True).csv(
    "hdfs://namenode:9000/user/root/job_skill/job_skill-240722-1822.csv"
)
df_job_skill.show(n=20, truncate=True)
df_job_skill.printSchema()
df_job_skill.groupBy("skill").count().show(n=20, truncate=False)

df_job_clean = df.select(
    # regexp_replace('title', r'[0-9]', ''), "title"
    trim(regexp_replace('title', r"\([^()]*\)|\~|\$|[0-9]|\bUp.*\b|\- |\.|\bGấp[a-zA-Z]*|\.\bUrgen.*\b|  +", '')).alias("title_clean"), "title"
    
)
df_job_clean.show(n=25, truncate=False)



