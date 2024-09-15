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

# Script generated for node Amazon S3
AmazonS3_node1723971384795 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-udacity/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1723971384795",
)

# Script generated for node Amazon S3
AmazonS3_node1723971396226 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-udacity/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1723971396226",
)

# Script generated for node Join
Join_node1723971429561 = Join.apply(
    frame1=AmazonS3_node1723971396226,
    frame2=AmazonS3_node1723971384795,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1723971429561",
)

# Script generated for node Drop Fields
DropFields_node1723971902491 = DropFields.apply(
    frame=Join_node1723971429561,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
    ],
    transformation_ctx="DropFields_node1723971902491",
)

# Script generated for node Amazon S3
AmazonS3_node1723972015333 = glueContext.getSink(path="s3://project-udacity/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1723972015333")
AmazonS3_node1723972015333.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1723972015333.setFormat("json")
AmazonS3_node1723972015333.writeFrame(DropFields_node1723971902491)
job.commit()
