from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col

spark = SparkSession.builder.appName("Transform").getOrCreate()

df = spark.read.option("header", True).csv(
    "hdfs://namenode:9000/user/root/job/job-190722-1752.csv"
)
df.show(n=20, truncate=True)
df.printSchema()

df.select("skills").show(n=5, truncate=False)

company_df = df.select(
    [
        monotonically_increasing_id().alias("company_id"),
        col("company").alias("company_name"),
        col("city"),
    ]
).distinct()

company_df.explain()
company_df.show(truncate=False)
