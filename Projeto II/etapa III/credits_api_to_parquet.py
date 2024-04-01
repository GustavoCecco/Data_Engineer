import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()

df = spark.read.json(
    "glue://dll_trusted_zone/20231105133906_json"
)

# Convertendo tipos de dados
df = df.select(
    df["id"].cast(IntegerType()),
    df["original_title"].cast(StringType()),
    df["release_date"].cast(StringType()),
    df["title"].cast(StringType()),
    df["vote_average"].cast(DoubleType()),
    df["vote_count"].cast(IntegerType()),
    df["character"].cast(StringType()),
    df["credit_id"].cast(StringType()),
    df["media_type"].cast(StringType()),
)

df.write.parquet(
    "s3://data-lake-exemplo/Trusted/parquet/2023/11/05/",
    mode="overwrite",
    compression="snappy",
)

spark.stop()
