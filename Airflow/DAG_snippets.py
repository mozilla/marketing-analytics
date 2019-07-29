#1. Import packages and modules

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
import logging

import snippetsMetaDataLoadJob

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')

#2. Set Default Arguments
default_args = {
    'owner': 'George Kaberere',
    'start_date': datetime(2019,7,26),
    #'end_date': datetime(2020,1,1),
    'depends_on_past': False,
    'email': ['gkaberere@mozilla.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

#3. Instantiate a DAG
dag = DAG(
    'Snippets_Data_Transfer',
    default_args=default_args,
    description='Pulls metadata from S3, Telemetry from redash, loads into BQ and creates summary table for dashboard',
    schedule_interval=None
)


#4. Tasks
retrieve_s3_file = PythonOperator(task_id='Retrieve_S3_file_load_GCS',
                                  python_callable=snippetsMetaDataLoadJob.air_flow_ingest_s3_file,
                                  dag=dag)

upload_to_BQ_table = PythonOperator(task_id='Ingest_data_into_BQ',
                                    python_callable=snippetsMetaDataLoadJob.airflow_upload_to_bq,
                                    dag=dag)

#5. Set up Dependencies
retrieve_s3_file >> upload_to_BQ_table