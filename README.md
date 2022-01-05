# Carsten - AWS Glue ETL

============

AWS Glue is a service for performing ETL tasks.
It is not programming free, but it simplifies work of Data Engineer.
There are four main components of AWS Glue:
1. Crawler - build metadata for store and connectors.
2. AWS Catalog - metadata store.
3. Visual editor - much of the work for inserting data to into staging area has been done using this product.
4. ETL Jobs - that generates code. You need to adapt it to fit your needs, but it removes a lot
of boilerplate coding.

AWS Catalog can be queried using AWS Athena. In other words underlying data is in S3, but you can
use SQL to query data in different formats including Parquet and CSV.

ETL Jobs generates code for ETL, but it requires some tuning. I think I spend a fair amount of time
learning how `awsglue` Python library work before saying that it is overly complicated.

AWS Glue Python library is wrapper around Apache Spark.
In order to access low level functions you need to convert it to Apache Spark DataFrame, make transforms
and then convert it back to DynamicFrame.
---

## Features
- AWS Glue support
- AWS Athena support

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