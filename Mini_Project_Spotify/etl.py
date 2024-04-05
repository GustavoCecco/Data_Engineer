import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Artist
Artist_node1712275041377 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-raw/raw/artists.csv"], "recurse": True}, transformation_ctx="Artist_node1712275041377")

# Script generated for node Album
Album_node1712275045252 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-raw/raw/albums.csv"], "recurse": True}, transformation_ctx="Album_node1712275045252")

# Script generated for node track
track_node1712275046532 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-raw/raw/track.csv"], "recurse": True}, transformation_ctx="track_node1712275046532")

# Script generated for node Join Album/Artist
JoinAlbumArtist_node1712275271929 = Join.apply(frame1=Album_node1712275045252, frame2=Artist_node1712275041377, keys1=["artist_id"], keys2=["id"], transformation_ctx="JoinAlbumArtist_node1712275271929")

# Script generated for node Join / tracks
Jointracks_node1712275369715 = Join.apply(frame1=track_node1712275046532, frame2=JoinAlbumArtist_node1712275271929, keys1=["track_id"], keys2=["track_id"], transformation_ctx="Jointracks_node1712275369715")

# Script generated for node Drop Fields
DropFields_node1712275435760 = DropFields.apply(frame=Jointracks_node1712275369715, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1712275435760")

# Script generated for node Destination
Destination_node1712275481638 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1712275435760, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-spotify-raw/cleaned/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1712275481638")

job.commit()