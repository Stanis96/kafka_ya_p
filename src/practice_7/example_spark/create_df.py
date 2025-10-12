from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StructField, StructType


spark = SparkSession.builder.master("local").appName("Learning DataFrames").getOrCreate()


data = [
    ("2021-01-04", 3744, 63, 322),
    ("2021-01-04", 2434, 21, 382),
    ("2021-01-04", 2434, 32, 159),
    ("2021-01-04", 3744, 32, 159),
    ("2021-01-04", 4342, 32, 159),
    ("2021-01-04", 4342, 12, 259),
    ("2021-01-04", 5677, 12, 259),
    ("2021-01-04", 5677, 23, 499),
]


schema = StructType(
    [
        StructField("longitude", FloatType(), nullable=True),
        StructField("latitude", FloatType(), nullable=True),
        StructField("median_age", FloatType(), nullable=True),
        StructField("total_rooms", FloatType(), nullable=True),
        StructField("total_bdrms", FloatType(), nullable=True),
        StructField("population", FloatType(), nullable=True),
        StructField("households", FloatType(), nullable=True),
        StructField("median_income", FloatType(), nullable=True),
        StructField("median_house_value", FloatType(), nullable=True),
    ],
)


df = spark.createDataFrame(data=data, schema=schema)
