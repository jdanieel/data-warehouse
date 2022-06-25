# Data Warehouse

## Introduction
This repo contains files required to run an ETL process in order to create two staging tables and five final tables into and AWS Redshift cluster. They are part of the third project required in Udacity's Data Engineering Nanodegree, called Data Warehouse.

## Context
In this project, as the Data Engineer of a fictional music startup called Sparkify, I had to create a database into a Redshift cluster using .json files containing song and songplays data in order to provide tables in which the analytics team can perform some queries to understand better users' behaviour. In this process, I created the tables' schemas and wrote the scripts in Python used for the ETL pipeline, using best practices in terms of documentation, data quality checks and code writing.

## Files

The repo contains the following files:

* `dwh.cfg`: config file that stores the AWS credentials and info necessary to access the Redshift cluster;
* `create_tables.py`: script that drops and creates the database tables;
* `etl.py`: script that copies the data from the `.json` files located at an S3 bucket into two staging tables (`stg_events` and `stg_songs`) on Redshift.Then, it also inserts the data from staging tables into the five final tables, making the necessary adjustments on data;
* `sql_queries.py`: file containing all the SQL queries that will be used in the scripts to drop, create, copy and insert data into the tables;
* `README.md`: the file you are reading right now :)

## Running the Scripts

### Dependencies
You need to have the library `psycopg2` installed at your environment in order to make the connection with your Postgres database. The easiest way to do this is using pip install, executing this on your terminal:

```pip install psycopg2-binary```

More info about this process can be found at the [psycopg2 documentation](https://www.psycopg.org/docs/install.html).

### Config file
In order to run these scripts, it's also necessary to edit the config file with the AWS credentials and cluster informations

### Steps
In order to properly run the scripts, do as follows:

* First, run `create_tables.py` in order to create the database and the tables that are necessary for the ETL processes;
* Second, run `etl.py` to read and process the data from the S3 bucket files. Make sure to have `sql_queries.py` at the same directory as `etl.py` and also to add the necessary info to `dwh.cfg` file.
