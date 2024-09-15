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
AmazonS3_node1723969712389 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-udacity/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1723969712389",
)

# Script generated for node Amazon S3
AmazonS3_node1723969762831 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-udacity/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1723969762831",
)

# Script generated for node Join
Join_node1723969832198 = Join.apply(
    frame1=AmazonS3_node1723969712389,
    frame2=AmazonS3_node1723969762831,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1723969832198",
)

# Script generated for node Drop Fields
DropFields_node1723936289173 = DropFields.apply(
    frame=Join_node1723969832198,
    paths=[
        "`.serialNumber`",
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
    transformation_ctx="DropFields_node1723936289173",
)

# Script generated for node Amazon S3
AmazonS3_node1723969772813 = glueContext.getSink(path="s3://project-udacity/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1723969772813")
AmazonS3_node1723969772813.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1723969772813.setFormat("json")
AmazonS3_node1723969772813.writeFrame(DropFields_node1723936289173)
job.commit()
