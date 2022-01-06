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

## File structure


---


## Usage
1. Upload Immigration dataset from `data/sas-data` to S3. Remember to upload only `parquet` files
as it causes errors in AWS Glue while loading data.
2. Upload Airport Codes dataset from `data/`.
3. Construct in AWS Glue Visual Editor necessary steps to make a mapping from S3 to AWS Catalog.
You can query data later with AWS Athena.
4. Open ETL Jobs and setup ETL Pipeline.
5. Upload code from this repository to ETL Jobs Pipeline.
Enjoy simplified ETL process.

---

## Explanation

---

## License
This project is licensed under the terms of the **MIT** license.