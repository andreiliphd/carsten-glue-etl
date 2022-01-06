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
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://andreiliphd-glue/airport-codes_csv.csv"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("ident", "string", "ident", "string"),
        ("type", "string", "type", "string"),
        ("name", "string", "name", "string"),
        ("elevation_ft", "long", "elevation_ft", "long"),
        ("continent", "string", "continent", "string"),
        ("iso_country", "string", "iso_country", "string"),
        ("iso_region", "string", "iso_region", "string"),
        ("municipality", "string", "municipality", "string"),
        ("gps_code", "string", "gps_code", "string"),
        ("iata_code", "string", "iata_code", "string"),
        ("local_code", "string", "local_code", "string"),
        ("coordinates", "string", "coordinates", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Data Catalog table
DataCatalogtable_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="immigration",
    table_name="airport_codes_csv_csv",
    transformation_ctx="DataCatalogtable_node3",
)

job.commit()
