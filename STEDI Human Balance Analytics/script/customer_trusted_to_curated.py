import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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
        "paths": ["s3://project-udacity/accelerometer/trusted/"],
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
    frame1=AmazonS3_node1723971384795,
    frame2=AmazonS3_node1723971396226,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1723971429561",
)

# Script generated for node Drop Fields
DropFields_node1723932148905 = DropFields.apply(
    frame=Join_node1723971429561,
    paths=["x", "y", "z", "user", "timestamp"],
    transformation_ctx="DropFields_node1723932148905",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1723939772121 = DynamicFrame.fromDF(
    DropFields_node1723932148905.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1723939772121",
)

# Script generated for node Amazon S3
AmazonS3_node1723972012732 = glueContext.getSink(path="s3://project-udacity/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1723972012732")
AmazonS3_node1723972012732.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1723972012732.setFormat("json")
AmazonS3_node1723972012732.writeFrame(DropDuplicates_node1723939772121)
job.commit()
