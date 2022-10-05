from pyspark import SparkConf
from pyspark.sql import SparkSession


def init_spark_session() -> SparkSession:
    master = "spark://spark:7077"

    conf = (
        SparkConf()
        .setAppName("Transform")
        .setMaster(master)
        .set("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .set("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "10g")
        .set("spark.executor.memory", "4g")
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        # .set("spark.sql.session.timeZone", "UTC+7")
        .set("spark.network.timeout", "50000")
        .set("spark.executor.heartbeatInterval", "5000")
        # .set("spark.worker.timeout", "5000")
    )

    spark = (
        SparkSession.builder.config(conf=conf)
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark
