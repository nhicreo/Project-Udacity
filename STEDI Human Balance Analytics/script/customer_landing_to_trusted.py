import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1723969639067 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-udacity/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1723969639067",
)

# Script generated for node Filter
Filter_node1723969754432 = Filter.apply(
    frame=AmazonS3_node1723969639067,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1723969754432",
)

# Script generated for node Amazon S3
AmazonS3_node1723969780261 = glueContext.getSink(path="s3://project-udacity/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1723969780261")
AmazonS3_node1723969780261.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AmazonS3_node1723969780261.setFormat("json")
AmazonS3_node1723969780261.writeFrame(Filter_node1723969754432)
job.commit()
