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
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": ["s3://andreiliphd-glue/sas_data/"], "recurse": True},
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("cicid", "double", "cicid", "double"),
        ("i94yr", "double", "i94yr", "double"),
        ("i94mon", "double", "i94mon", "double"),
        ("i94cit", "double", "i94cit", "double"),
        ("i94res", "double", "i94res", "double"),
        ("i94port", "string", "i94port", "string"),
        ("arrdate", "double", "arrdate", "double"),
        ("i94mode", "double", "i94mode", "double"),
        ("i94addr", "string", "i94addr", "string"),
        ("depdate", "double", "depdate", "double"),
        ("i94bir", "double", "i94bir", "double"),
        ("i94visa", "double", "i94visa", "double"),
        ("count", "double", "count", "double"),
        ("dtadfile", "string", "dtadfile", "string"),
        ("visapost", "string", "visapost", "string"),
        ("occup", "string", "occup", "string"),
        ("entdepa", "string", "entdepa", "string"),
        ("entdepd", "string", "entdepd", "string"),
        ("entdepu", "string", "entdepu", "string"),
        ("matflag", "string", "matflag", "string"),
        ("biryear", "double", "biryear", "double"),
        ("dtaddto", "string", "dtaddto", "string"),
        ("gender", "string", "gender", "string"),
        ("insnum", "string", "insnum", "string"),
        ("airline", "string", "airline", "string"),
        ("admnum", "double", "admnum", "double"),
        ("fltno", "string", "fltno", "string"),
        ("visatype", "string", "visatype", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Data Catalog table
DataCatalogtable_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="udacity",
    table_name="sas_data",
    transformation_ctx="DataCatalogtable_node3",
)

job.commit()
