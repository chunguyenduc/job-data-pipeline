from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Transform").getOrCreate()

df = spark.read.option("header", True).csv(
    "hdfs://namenode:9000/user/root/job/job-190722-1240.csv"
)
df.show(n=20, truncate=True)
df.printSchema()

df.select("skills").show(n=5, truncate=False)
