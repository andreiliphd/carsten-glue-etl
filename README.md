# Carsten - AWS Glue ETL

============

AWS Glue is a service for performing ETL tasks.
There are four main components of AWS Glue:
1. Crawler - build metadata for store and connectors.
2. AWS Catalog - metadata store.
3. AWS Glue Studio - much of the work for inserting data to into staging area has been done using this product.
4. ETL Jobs - that generates code. You need to adapt it to fit your needs, but it removes a lot
of boilerplate coding.

AWS Catalog can be queried using AWS Athena. In other words underlying data is in S3, but you can
use SQL to query data in different formats including Parquet and CSV.

ETL Jobs generates code for ETL, but it requires some tuning. I think I spend a fair amount of time
learning how `awsglue` Python library work before saying that it is overly complicated.

AWS Glue Python library is wrapper around Apache Spark.
In order to access low level functions you need to convert it to Apache Spark DataFrame, make transforms
and then convert it back to DynamicFrame.

AWD DataBrew is a product aimed to simplify life of data scientists and data engineers in performing data 
quality checks and data cleaning operations.

---

## Project Summary
Project performs ETL on `I94 Immigration Data 2016`, `U.S. City Demographic Data` and `Airport Codes Table` datasets. 

## Scope
ETL: S3 -> AWS Catalog -> AWS RDS(PostgreSQL)
ETL to preprocess data for analyzing airports used by immigrants in US.

## Queries
Analytics queries include:
- Which airport immigrants used the most to travel to?
- Which airport immigrants used the most to travel from?
- What are top most airports used by immigrants?

## Datasets
Following datasets are used for this project:
1. `I94 Immigration Data 2016`: US National Tourism and Trade Office
    - Source: [https://travel.trade.gov/research/reports/i94/historical/2016.html](https://travel.trade.gov/research/reports/i94/historical/2016.html)
    - Number of rows after ETL: 3096313
    - Format: Parquet
2. `U.S. City Demographic Data`: OpenSoft
    - Source: [https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)
    - Number of rows after ETL: 2891
    - Format: CSV
3. `Airport Codes Table`: Datahub
    - Source: [https://datahub.io/core/airport-codes#data](https://datahub.io/core/airport-codes#data)
    - Number of rows after ETL: 9189
    - Format: CSV

Total number of rows after ETL: 3108393.
For more details, please, check `size_of_database.ipynb`.
You can also find number of rows in database from [Data Quality](#data-quality) section of this file.

## Data Exploration
Data Exploration was performed using AWS Athena.
Conclusions:
1. Some columns are not needed as they mostly contained NULL values.
2. Data type for some columns was changed.
3. Date data type was added for `arrdata` and `depdate`.
 
## Motivation
### Product
There are three main products that I considered before executing project:
1. Apache Spark
2. Apache Airflow
3. AWS Glue

#### Apache Spark
Pluses:
- RDD is an effective mechanism for working with big datasets
- SQL engine and Dataset abstractions for performing data analytics and ETL
Minuses:
- No bash operators

#### Apache Airflow
Pluses:
- Written in Python
- Variety of built-in operators
Minuses:
- No analytics engine

#### AWS Glue
Pluses:
- Cost-effective
- Code generation support
- Clear documentation
- Ability to adapt generated code
Minuses:
- Performing rare low level operations can be complicated

### Project
The aim of the project is to analyze airports immigrants use most.

## Features
- AWS Athena support
- AWS Glue support
- AWS Glue DataBrew support

---

## Setup
Clone this repo:
```
git@github.com:andreiliphd/carsten-glue-etl.git
```

---

## File and folder structure
`setup_database.py` - creating database and dropping tables if exists.

`sql_queries.py` - SQL Queries

`test.ipynb` - test functionality

`data-quality-checks/` - reports on data quality

`data/` - datasets

`scripts/` - ETL scripts

---


## Usage
1. Create configuration file `dwh.cfg`:
```
[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=

[IMMIGRATION]
DB_NAME=immigration
```
2.Upload Immigration dataset from `data/sas-data` to S3. Remember to upload only `parquet` files
as it causes errors in AWS Glue while loading data.
3. Upload Airport Codes(`airport-codes_csv.csv`) dataset from `data/` to S3.
4. Upload State Codes(`us-city-demographics.csv`) dataset from `data/` to S3.
5. Upload code to AWS Glue Visual Editor.
6. Upload code from this repository to ETL Jobs.
Enjoy simplified ETL process.

---
## Workflow
ETL steps:
1. Loading data from S3 to AWS Catalog.
2. Cleaning data.
3. Moving data to PostgreSQL.

The whole process is automatic and can be scheduled, run on demand or triggered by event.
![workflow](https://github.com/andreiliphd/carsten-glue-etl/blob/master/screenshots/workflow.png?raw=true)

## Explanation
ETL performed in AWS Glue.
Data quality checks performed in AWS DataBrew.
Schema is exported from JetBrains DataGrip.



## Data Dictionary
### List databases
```
[(1, 'template1', True, -1),
 (14300, 'template0', False, -1),
 (14301, 'postgres', True, -1),
 (16384, 'rdsadmin', True, -1),
 (16607, 'immigration', True, -1)]
```
### List tables
```
[('public', 'airport_codes'),
 ('public', 'immigration'),
 ('public', 'state_codes')]
```
---
Star Schema was used when designing a database.
![schema](https://github.com/andreiliphd/carsten-glue-etl/blob/master/schema/schema.png)

Data model is simple and suitable for further analysis in BI tools such as Tableau or Metabase.

## Scenarios
- The data was increased by 100x.
Two dimensions that we have to consider: compute and storage.
We can add more nodes to raise processing power of ETL pipeline scaling horizontally, 
increasing concurrency.
We can add more storage to instances scaling vertically, increasing storage capacity.
- The pipelines would be run on a daily basis by 7 am every day.
There is a scheduling functionality in AWS Glue.
- The database needed to be accessed by 100+ people.
We can scale vertically or/and horizontally. 
Vertical scaling is an older approach. Programming concepts developed dramatically that
allow to scale horizontally without problems. You can add Elastic Load Balancer and 
choose different strategies for distributing your traffic across replicas in AWS RDS.
I also want to mention that S3 is growing very fast and AWS Athena allows working
with more comfort even with CSV data. You can offload some heavy tasks from PostgreSQL
to S3. And query your data with AWS Athena.
Regarding user management there is an excellent article on 
[AWS Blog](https://aws.amazon.com/ru/blogs/database/managing-postgresql-users-and-roles/).
We should create database roles, users and group. We can also use IAM for authentication.
RDS Parameter Groups can be used to customize monitoring of user activity.

## Data Quality
### Airport codes
Full [report](https://github.com/andreiliphd/carsten-glue-etl/blob/master/data-quality-reports/airport-codes_74003fcc5eb44b85c6f6e5802979f5cc67c3d5186c09e1056681e74e1b4a5161.json).

![airport-codes](https://github.com/andreiliphd/carsten-glue-etl/blob/master/data-quality-reports/airport-codes%20profile%20job_2022-01-06-02_51_06.png?raw=true)

### State codes
Full [report](https://github.com/andreiliphd/carsten-glue-etl/blob/master/data-quality-reports/state-codes_7e9ad332a45e3b174b68c634396d762cf206ce2f04706e6a31b9f750121c91ad.json).

![state-codes](https://github.com/andreiliphd/carsten-glue-etl/blob/master/data-quality-reports/state-codes%20profile%20job_2022-01-06-12_04_25.png?raw=true)

### Immigration
Full [report](https://github.com/andreiliphd/carsten-glue-etl/blob/master/data-quality-reports/immigration_2435c2a91f69448a135f1a5d93b2b81d0b88005c02b33c20c9c44b98576ac7d9.json).

![immigration](https://github.com/andreiliphd/carsten-glue-etl/blob/master/data-quality-reports/immigration%20profile%20job_2022-01-06-12_06_42.png?raw=true)


## License
This project is licensed under the terms of the **MIT** license.