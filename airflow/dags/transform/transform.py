from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, TimestampType
from pyspark.sql.functions import col, regexp_replace, trim, lag

# spark = SparkSession.builder.\
#     appName("Transform").\
#     setMaster("spark://spark:7077").\
#     enableHiveSupport().\
#     getOrCreate()
master = "spark://spark:7077"
conf = SparkConf().setAppName("Transform").setMaster(master). \
    set("spark.memory.offHeap.enabled", True).\
    set("spark.memory.offHeap.size", "10g")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).config("spark.memory.offHeap.enabled","true")\
    .config("spark.memory.offHeap.size","10g").enableHiveSupport().getOrCreate()


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
    .csv("hdfs://namenode:9000/user/root/job/job-260722-1713.csv")
)
df.show(n=20, truncate=True)
df.printSchema()

df_job_skill = spark.read.option("header", True).csv(
    "hdfs://namenode:9000/user/root/job_skill/job_skill-260722-1713.csv"
)
df_job_skill.show(n=20, truncate=True)
df_job_skill.printSchema()
df_job_skill.groupBy("skill").count().show(n=20, truncate=False)

# df_job_clean = df.select(
#     # regexp_replace('title', r'[0-9]', ''), "title"
#     # trim(
#     #     regexp_replace(
#     #         "title",
#     #         r"\([^()]*\)|\~|\$|([0-9])|\bUp.*\b|\- |\.|\bGấp[a-zA-Z]*|\.\bUrg.*\b|  +",
#     #         "",
#     #     )
#     # ).alias("title_clean"),
#     col("title")
#     col("id"),
#     col("city"),
#     col("company"),
#     col("created_date"),
# )
# df_job_clean.show(n=30, truncate=False)

spark.sql("CREATE DATABASE IF NOT EXISTS staging")
spark.sql("DROP TABLE IF EXISTS staging.job_info")
# with  open("/opt/airflow/dags/transform/create_table.sql") as fr:
with  open("create_table.sql") as fr:
    query = fr.read()
    print(query)
spark.sql(query)
df.write.mode("append").format("hive").saveAsTable("staging.job_info")
spark.sql("select * from staging.job_info").show(n=50, truncate=True)
