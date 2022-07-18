from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Transform").getOrCreate()

df = spark.read.option("header", True).csv(
    "file:////home/duc/Documents/job-dashboard/airflow/dags/transform/job-130722-1910.csv"
)
df.show(n=20, truncate=True)
df.printSchema()

df.select("skills").show(n=5, truncate=False)
