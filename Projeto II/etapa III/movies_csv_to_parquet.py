import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def transform_data(df):
  df = df.drop("anofalecimento", "anonascimento", "profissao", "generoartista", "tempominutos", "titulopincipal", "titulosmaisconhecidos", "anolancamento")
  df["titulo"] = df["titulo"].cast("string")
  df = df.filter(df["anolancamento"] >= 1990)

  return df

# Executar pipeline ETL
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Ler dados do Amazon S3
AmazonS3_node1699198282863 = glueContext.create_dynamic_frame.from_catalog(
    database="dll_trusted_zone",
    table_name="movies_csv",
    transformation_ctx="AmazonS3_node1699198282863",
)

AmazonS3_node1699199413134 = transform_data(AmazonS3_node1699198282863)

# Escrever dados no Amazon S3
glueContext.write_dynamic_frame.from_options(
    frame=AmazonS3_node1699199413134,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://data-lake-exemplo/Trusted/parquet/2023/11/05/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1699199413134",
)

job.commit()
