# etl/spark_session.py
from pyspark.sql import SparkSession

def get_spark_session(app_name="MySparkApp"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages",
                "org.postgresql:postgresql:42.6.0,"
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
        .getOrCreate()
    )
