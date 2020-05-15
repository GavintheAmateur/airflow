# Sparkify Songplay Data ETL   

## Background  
This project build a data pipeline for Sparkify using Apache Airflow to automatically and periodically pull user behavior data in Amazon s3 and load them into formalized databases in readshift for analysis.  
## Architecture  
### Project structure:  
- data/: contains all raw data files;
- create_tables.py: script to set up the normalized schema;
- etl.ipynb: place to tap and transform data before massive processing;
- etl.py: program to structurize and apply processes built through etl.ipynb;  
- sql_queries.py: all-in-one file for sql queries used;
- test.ipynb: test if ETL is being built correctly at any time

## Usage:  
1. do ETL:
in commandline at project root, run:
python create_tables.py && python etl.py

2. Analysis Example:   
```
%sql select * from songplays where artist_id is not null 
```
-- this query is supposed to return only one row of songplay record given the sub-dataset the project provides.