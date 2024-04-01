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

AWSGlueDataCatalog_node1699200204035 = glueContext.create_dynamic_frame.from_catalog(
    database="dll_trusted_zone",
    table_name="20231105130127_json",
    transformation_ctx="AWSGlueDataCatalog_node1699200204035",
)

ChangeSchema_node1699200840353 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1699200204035,
    mappings=[("id", "int", "id", "int"), ("name", "string", "name", "string")],
    transformation_ctx="ChangeSchema_node1699200840353",
)

AmazonS3_node1699200955619 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1699200840353,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://data-lake-exemplo/Trusted/parquet/2023/11/05/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1699200955619",
)

job.commit()
