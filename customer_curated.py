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

# Script generated for node Landing accelerometer data
Landingaccelerometerdata_node1685155291073 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="tharso",
        table_name="accelerometer_landing",
        transformation_ctx="Landingaccelerometerdata_node1685155291073",
    )
)

# Script generated for node Trusted user data
Trusteduserdata_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="tharso",
    table_name="customer_trusted",
    transformation_ctx="Trusteduserdata_node1",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1685219048189 = DynamicFrame.fromDF(
    Landingaccelerometerdata_node1685155291073.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1685219048189",
)

# Script generated for node Join
Join_node1685155384579 = Join.apply(
    frame1=Trusteduserdata_node1,
    frame2=DropDuplicates_node1685219048189,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1685155384579",
)

# Script generated for node Drop variables
Dropvariables_node1685155493559 = ApplyMapping.apply(
    frame=Join_node1685155384579,
    mappings=[
        ("customername", "string", "customername", "string"),
        ("email", "string", "email", "string"),
        ("phone", "string", "phone", "string"),
        ("birthday", "string", "birthday", "string"),
        ("serialnumber", "string", "serialnumber", "string"),
        ("registrationdate", "long", "registrationdate", "long"),
        ("lastupdatedate", "long", "lastupdatedate", "long"),
        ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"),
        ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"),
        ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long"),
    ],
    transformation_ctx="Dropvariables_node1685155493559",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1685228567478 = DynamicFrame.fromDF(
    Dropvariables_node1685155493559.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1685228567478",
)

# Script generated for node Trusted accelerometer
Trustedaccelerometer_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1685228567478,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tharsos-bucket/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="Trustedaccelerometer_node3",
)

job.commit()
