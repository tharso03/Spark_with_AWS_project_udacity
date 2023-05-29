import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Landing customer data
Landingcustomerdata_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://tharsos-bucket/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Landingcustomerdata_node1",
)

# Script generated for node Privacy filter
Privacyfilter_node1685128020981 = Filter.apply(
    frame=Landingcustomerdata_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Privacyfilter_node1685128020981",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1685210652039 = DynamicFrame.fromDF(
    Privacyfilter_node1685128020981.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1685210652039",
)

# Script generated for node Trusted customer zone
Trustedcustomerzone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1685210652039,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tharsos-bucket/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Trustedcustomerzone_node3",
)

job.commit()
