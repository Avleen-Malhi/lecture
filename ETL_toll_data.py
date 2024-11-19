# Import libraries
import os
import shutil
from datetime import datetime, timedelta

import boto3
import duckdb

from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

# Task 1.1 - Define DAG arguments
default_args = {
    'owner': 'Avleen Malhi',
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

# Task 1.2 - Define the DAG
dag = DAG(
    dag_id='ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)

# Task 1.3 - Unzip
unzip_data = BashOperator(
    task_id= 'unzip_data',
    bash_command= 'tar -xzvf /opt/airflow/data/tolldata.tgz -C /opt/airflow/data',
    dag= dag,
)

# Task 1.4 - Extract Data from CSV
extract_data_from_csv = BashOperator(
    task_id= 'extract_data_from_csv',
    bash_command= 'cut -d"," -f1-4 < /opt/airflow/data/vehicle-data.csv > /opt/airflow/data/csv_data.csv',
    dag= dag,
)

# Task 1.5 - Extract Data from TSV
extract_data_from_tsv = BashOperator(
    task_id= 'extract_data_from_tsv',
    bash_command= 'cut -f5-7 < /opt/airflow/data/tollplaza-data.tsv > /opt/airflow/data/tsv_data.csv',
    dag= dag,
)

# Task 1.6 - Extract Data from Fixed width
extract_data_from_fixed_file = BashOperator(
    task_id= 'extract_data_from_fixed_file',
    bash_command= 'cut -c 59-68 < /opt/airflow/data/payment-data.txt > /opt/airflow/data/fixed_width_data.csv',
    dag= dag,
)

# Task 1.7 - Consolidate Data
consolidate_data = BashOperator(
    task_id= 'consolidate_data',
    bash_command= 'paste /opt/airflow/data/vehicle-data.csv /opt/airflow/data/tollplaza-data.tsv /opt/airflow/data/payment-data.txt > /opt/airflow/data/extracted_data.csv',
    dag= dag,
)

# Task 1.8 - Transform Data
Transform_and_load_data = BashOperator(
    task_id= 'Transform_and_load_data',
    bash_command= 'tr "[a-z]" "[A-Z]" < /opt/airflow/data/extracted_data.csv',
    dag= dag,
)


# Task 1.9 - Create pipeline

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_file >> consolidate_data >> Transform_and_load_data

