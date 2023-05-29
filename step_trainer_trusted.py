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

# Script generated for node Step trainer landing data
Steptrainerlandingdata_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="tharso",
    table_name="step_trainer_landing",
    transformation_ctx="Steptrainerlandingdata_node1",
)

# Script generated for node Curated customer data
Curatedcustomerdata_node1685235562770 = glueContext.create_dynamic_frame.from_catalog(
    database="tharso",
    table_name="customer_curated",
    transformation_ctx="Curatedcustomerdata_node1685235562770",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1685355727978 = ApplyMapping.apply(
    frame=Steptrainerlandingdata_node1,
    mappings=[
        ("sensorreadingtime", "long", "`(right) sensorreadingtime`", "long"),
        ("serialnumber", "string", "`(right) serialnumber`", "string"),
        ("distancefromobject", "int", "`(right) distancefromobject`", "int"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1685355727978",
)

# Script generated for node Join
Join_node1685235603423 = Join.apply(
    frame1=Curatedcustomerdata_node1685235562770,
    frame2=RenamedkeysforJoin_node1685355727978,
    keys1=["serialnumber"],
    keys2=["`(right) serialnumber`"],
    transformation_ctx="Join_node1685235603423",
)

# Script generated for node Change Schema
ChangeSchema_node1685355763021 = ApplyMapping.apply(
    frame=Join_node1685235603423,
    mappings=[
        ("email", "string", "email", "string"),
        ("`(right) sensorreadingtime`", "long", "sensorreadingtime", "long"),
        ("`(right) serialnumber`", "string", "serialnumber", "string"),
        ("`(right) distancefromobject`", "int", "distancefromobject", "int"),
    ],
    transformation_ctx="ChangeSchema_node1685355763021",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1685356425855 = DynamicFrame.fromDF(
    ChangeSchema_node1685355763021.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1685356425855",
)

# Script generated for node Trusted step data
Trustedstepdata_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1685356425855,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tharsos-bucket/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Trustedstepdata_node3",
)

job.commit()
