# Sparkify Songplay Data ETL   

## Background  
This project build a data pipeline for Sparkify using Apache Airflow to automatically and periodically pull user behavior data in Amazon s3 and load them into formalized databases in readshift for analysis.  
## Architecture  
### Project structure:  
- dags/sparkify_etl_dag.py: defines the dag for Sparkify data etl;  
- plugins/helpers/sql_queries.py: stores all sqls used in this project; 
- plugins/operators: stores all operators used in the dag.  

## Usage:  
1. pip install airflow  
2. Go to your $AIRFLOW_HOME holder, replace dags and plugins folder with the folders in this project.  
3. airflow initdb, airflow webserver, airflow scheduler, airflow worker  
4. Check and manage the dag on airflow webui (need to configure your AWS s3 and redshift credentials first if you run the dag).  