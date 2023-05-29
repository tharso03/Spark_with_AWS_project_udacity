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

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1685362585406 = glueContext.create_dynamic_frame.from_catalog(
    database="tharso",
    table_name="accelerometer_trusted",
    transformation_ctx="Accelerometertrusted_node1685362585406",
)

# Script generated for node Step trainer trusted
Steptrainertrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="tharso",
    table_name="step_trainer_trusted",
    transformation_ctx="Steptrainertrusted_node1",
)

# Script generated for node Join
Join_node1685362673179 = Join.apply(
    frame1=Steptrainertrusted_node1,
    frame2=Accelerometertrusted_node1685362585406,
    keys1=["email", "sensorreadingtime"],
    keys2=["user", "timestamp"],
    transformation_ctx="Join_node1685362673179",
)

# Script generated for node Drop variables
Dropvariables_node1685362755517 = ApplyMapping.apply(
    frame=Join_node1685362673179,
    mappings=[
        ("email", "string", "email", "string"),
        ("serialnumber", "string", "serialnumber", "string"),
        ("distancefromobject", "int", "distancefromobject", "int"),
        ("x", "float", "x", "float"),
        ("y", "float", "y", "float"),
        ("z", "float", "z", "float"),
        ("timestamp", "long", "timestamp", "long"),
    ],
    transformation_ctx="Dropvariables_node1685362755517",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Dropvariables_node1685362755517,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tharsos-bucket/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
