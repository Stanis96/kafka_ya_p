from pyspark.sql import SparkSession


spark = (
    SparkSession.builder.config("spark.executor.memory", "4g")
    .config("spark.executor.cores", 1)
    .appName("Python Spark basic example")
    .getOrCreate()
)
