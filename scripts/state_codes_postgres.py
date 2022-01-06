import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "udacity", table_name = "us_cities_demographics_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "udacity", table_name = "us_cities_demographics_csv", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("city", "string", "city", "string"), ("state", "string", "state", "string"), ("median age", "double", "median age", "double"), ("male population", "long", "male population", "long"), ("female population", "long", "female population", "long"), ("total population", "long", "total population", "long"), ("number of veterans", "long", "number of veterans", "long"), ("foreign-born", "long", "foreign-born", "long"), ("average household size", "double", "average household size", "double"), ("state code", "string", "state code", "string"), ("race", "string", "race", "string"), ("count", "long", "count", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("city", "string", "city", "string"), ("state", "string", "state", "string"), ("median age", "double", "median age", "double"), ("male population", "long", "male population", "long"), ("female population", "long", "female population", "long"), ("total population", "long", "total population", "long"), ("number of veterans", "long", "number of veterans", "long"), ("foreign-born", "long", "foreign-born", "long"), ("average household size", "double", "average household size", "double"), ("state code", "string", "state code", "string"), ("race", "string", "race", "string"), ("count", "long", "count", "long")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [catalog_connection = "udacity", connection_options = {"dbtable": "us_cities_demographics_csv", "database": "immigration"}, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "udacity", connection_options = {"dbtable": "state_codes", "database": "immigration"}, transformation_ctx = "datasink4")
job.commit()