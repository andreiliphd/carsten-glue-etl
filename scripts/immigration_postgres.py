import sys
from datetime import datetime
from datetime import timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import udf, regexp_replace
from pyspark.sql.functions import col,year,month,dayofmonth,to_date,from_unixtime

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "udacity", table_name = "sas_data", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "udacity", table_name = "sas_data", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("cicid", "double", "cicid", "double"), ("i94yr", "double", "i94yr", "double"), ("i94mon", "double", "i94mon", "double"), ("i94cit", "double", "i94cit", "double"), ("i94res", "double", "i94res", "double"), ("i94port", "string", "i94port", "string"), ("arrdate", "double", "arrdate", "integer"), ("i94mode", "double", "i94mode", "double"), ("i94addr", "string", "i94addr", "string"), ("depdate", "double", "depdate", "double"), ("i94bir", "double", "i94bir", "double"), ("i94visa", "double", "i94visa", "double"), ("count", "double", "count", "double"), ("dtadfile", "string", "dtadfile", "string"), ("visapost", "string", "visapost", "string"), ("occup", "string", "occup", "string"), ("entdepa", "string", "entdepa", "string"), ("entdepd", "string", "entdepd", "string"), ("entdepu", "string", "entdepu", "string"), ("matflag", "string", "matflag", "string"), ("biryear", "double", "biryear", "double"), ("dtaddto", "string", "dtaddto", "string"), ("gender", "string", "gender", "string"), ("insnum", "string", "insnum", "string"), ("airline", "string", "airline", "string"), ("admnum", "double", "admnum", "double"), ("fltno", "string", "fltno", "string"), ("visatype", "string", "visatype", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("cicid", "double", "cicid", "bigint"), ("i94yr", "double", "i94yr", "integer"), ("i94mon", "double", "i94mon", "integer"), ("i94cit", "double", "i94cit", "integer"), ("i94res", "double", "i94res", "integer"), ("i94port", "string", "i94port", "string"), ("arrdate", "double", "arrdate", "integer"), ("i94mode", "double", "i94mode", "integer"), ("i94addr", "string", "i94addr", "string"), ("depdate", "double", "depdate", "integer"), ("i94bir", "double", "i94bir", "integer"), ("i94visa", "double", "i94visa", "integer"), ("count", "double", "count", "bigint"), ("dtadfile", "string", "dtadfile", "string"), ("visapost", "string", "visapost", "string"), ("occup", "string", "occup", "string"), ("entdepa", "string", "entdepa", "string"), ("entdepd", "string", "entdepd", "string"), ("entdepu", "string", "entdepu", "string"), ("matflag", "string", "matflag", "string"), ("biryear", "double", "biryear", "integer"), ("dtaddto", "string", "dtaddto", "string"), ("gender", "string", "gender", "string"), ("insnum", "string", "insnum", "bigint"), ("airline", "string", "airline", "string"), ("admnum", "double", "admnum", "double"), ("fltno", "string", "fltno", "string"), ("visatype", "string", "visatype", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
# ## @type: Map
# ## @args: [f = convert_date, transformation_ctx = "convert_arr_date"]
# ## @return: convert_arr_date
# ## @inputs: [frame = resolvechoice2]
# def convert_date(x):
#     if x["arrdate"]:
#         x["arrdate"] = (datetime(1960, 1, 1).date() + timedelta(x["arrdate"])).strftime('%Y-%m-%d')
#     else:
#         x["arrdate"] = None
#     print("arrdate", x["arrdate"])
#     return x
# convert_arr_date = Map.apply(frame = resolvechoice2, f = convert_date, transformation_ctx = "convert_arr_date")


...

df = resolvechoice2.toDF()


def clean_immigration_data(immigration_df):
    get_isoformat_date = udf(lambda x: (datetime(1960, 1, 1).date() + timedelta(x)).isoformat() if x else None)
    get_valid_birth_year = udf(lambda yr: yr if (yr and 1900 <= yr <= 2016) else None)
    return immigration_df \
        .withColumn('arrdate', to_date(get_isoformat_date(immigration_df.arrdate), 'yyyy-MM-dd')) \
        .withColumn('depdate', to_date(get_isoformat_date(immigration_df.depdate), 'yyyy-MM-dd')) \
        .withColumn("biryear", get_valid_birth_year(immigration_df.biryear))


cleaned_immigration_df = clean_immigration_data(df)
logger.info("DataFrame " + str(cleaned_immigration_df))
dyf = DynamicFrame.fromDF(cleaned_immigration_df, glueContext, "enriched")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = dyf]
dropnullfields3 = DropNullFields.apply(frame = dyf, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [catalog_connection = "udacity", connection_options = {"dbtable": "sas_data", "database": "immigration"}, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "udacity", connection_options = {"dbtable": "immigration", "database": "immigration"}, transformation_ctx = "datasink4")
job.commit()