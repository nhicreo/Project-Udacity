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
AmazonS3_node1723969781293 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AmazonS3_node1723969781293",
)

# Script generated for node Amazon S3
AmazonS3_node1723969781293 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="AmazonS3_node1723969781293",
)

# Script generated for node Join
Join_node1723971982131 = Join.apply(
    frame1=AmazonS3_node1723969781293,
    frame2=AmazonS3_node1723969781293,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1723971982131",
)

# Script generated for node Amazon S3
AmazonS3_node1723969777132 = glueContext.getSink(path="s3://project-udacity/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1723969777132")
AmazonS3_node1723969777132.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1723969777132.setFormat("json")
AmazonS3_node1723969777132.writeFrame(Join_node1723971982131)
job.commit()
