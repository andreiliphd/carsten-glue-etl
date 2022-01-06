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
## @args: [database = "udacity", table_name = "airport_codes_csv_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "udacity", table_name = "airport_codes_csv_csv", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("ident", "string", "ident", "string"), ("type", "string", "type", "string"), ("name", "string", "name", "string"), ("elevation_ft", "long", "elevation_ft", "long"), ("continent", "string", "continent", "string"), ("iso_country", "string", "iso_country", "string"), ("iso_region", "string", "iso_region", "string"), ("municipality", "string", "municipality", "string"), ("gps_code", "string", "gps_code", "string"), ("iata_code", "string", "iata_code", "string"), ("local_code", "string", "local_code", "string"), ("coordinates", "string", "coordinates", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("ident", "string", "ident", "string"), ("type", "string", "type", "string"), ("name", "string", "name", "string"), ("elevation_ft", "long", "elevation_ft", "long"), ("continent", "string", "continent", "string"), ("iso_country", "string", "iso_country", "string"), ("iso_region", "string", "iso_region", "string"), ("municipality", "string", "municipality", "string"), ("gps_code", "string", "gps_code", "string"), ("iata_code", "string", "iata_code", "string"), ("local_code", "string", "local_code", "string"), ("coordinates", "string", "coordinates", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
## @type: DropFields
## @args: [paths = ["continent"], transformation_ctx = "drop_fields3"]
## @return: drop_fields3
## @inputs: [frame = resolvechoice2]
drop_fields3 = DropFields.apply(frame = resolvechoice2, paths = ["continent"], transformation_ctx = "drop_fields3")
## @type: Filter
## @args: [f = filter_iata_code, transformation_ctx = "filter_iata"]
## @return: filter_iata
## @inputs: [frame = drop_fields3]


def filter_iata_code(dynamicRecord):
	if dynamicRecord["iata_code"] != "":
		return True
	else:
		return False


filter_iata = Filter.apply(frame = drop_fields3, f = filter_iata_code, transformation_ctx = "filter_iata")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = filter_iata]
dropnullfields3 = DropNullFields.apply(frame = filter_iata, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [catalog_connection = "udacity", connection_options = {"dbtable": "airport_codes_csv_csv", "database": "postgres"}, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "udacity", connection_options = {"dbtable": "airport_codes", "database": "immigration"}, transformation_ctx = "datasink4")
job.commit()