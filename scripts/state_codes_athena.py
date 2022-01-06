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

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ";"},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://andreiliphd-glue/us-cities-demographics.csv"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("city", "string", "city", "string"),
        ("state", "string", "state", "string"),
        ("median age", "string", "median age", "double"),
        ("male population", "string", "male population", "long"),
        ("female population", "string", "female population", "long"),
        ("total population", "string", "total population", "long"),
        ("number of veterans", "string", "number of veterans", "long"),
        ("foreign-born", "string", "foreign-born", "long"),
        ("average household size", "string", "average household size", "double"),
        ("state code", "string", "state code", "string"),
        ("race", "string", "race", "string"),
        ("count", "string", "count", "long"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Data Catalog table
DataCatalogtable_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="udacity",
    table_name="us_cities_demographics_csv",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    transformation_ctx="DataCatalogtable_node3",
)

job.commit()
