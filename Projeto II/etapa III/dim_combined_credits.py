import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

AmazonS3_node1699203115843 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://data-lake-exemplo/Trusted/parquet/2023/11/05/run-1699201920409-part-block-0-r-00000-snappy.parquet"
        ],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1699203115843",
)

AmazonS3_node1699203138679 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonS3_node1699203115843,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://data-lake-exemplo/Refined/parquet/2023/11/05/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1699203138679",
)

job.commit()
