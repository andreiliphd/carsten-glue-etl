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
3. Upload Airport Codes dataset from `data/` to S3.
4. Upload State Codes(`us-city-demographics.csv`) data from `data` to S3.
5. Upload code to AWS Glue Visual Editor.
6. Upload code from this repository to ETL Jobs.
Enjoy simplified ETL process.

---

## Explanation
ETL performed in AWS Glue.
Data quality checks performed in AWS DataBrew.
Schema is exported from JetBrains DataGrip.

Star Schema was used when designing a database.
![schema](https://github.com/andreiliphd/carsten-glue-etl/blob/master/schema/schema.png)

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

## License
This project is licensed under the terms of the **MIT** license.