#1. Import packages and modules

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
import logging

import siteMetricsSummaryTable
import siteMetricsByLandingPageSummaryTable
import siteMetricsByPageSummaryTable
import blogsSiteMetricsByLandingPageSummaryTable
import blogsSiteMetricsSummaryTable

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')

#2. Set Default Arguments
default_args = {
    'owner': 'George Kaberere',
    'start_date': datetime(2019,11,7),
    #'end_date': datetime(2020,1,1),
    'depends_on_past': False,
    'email': ['gkaberere@mozilla.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

#3. Instantiate a DAG
dag = DAG(
    'Create_Site_Metrics_Summary_Table',
    default_args=default_args,
    description='Aggregates daily moz.org key metrics and creates summary table for dashboards and reporting',
    schedule_interval='00 16 * * *'
)


#4. Tasks
update_site_metrics_table = PythonOperator(task_id='create_daily_summary_table',
                                  python_callable=siteMetricsSummaryTable.run_site_metrics_update,
                                  dag=dag)

update_site_metrics_by_landing_page_table = PythonOperator(task_id='create_daily_summary_table_by_landing_page',
                                    python_callable=siteMetricsByLandingPageSummaryTable.run_site_metrics_landing_page_update,
                                    dag=dag)

update_site_metrics_by_page_table = PythonOperator(task_id='create_daily_summary_table_by_page',
                                    python_callable=siteMetricsByPageSummaryTable.run_site_metrics_update,
                                    dag=dag)

update_blog_metrics_table = PythonOperator(task_id='create_daily_summary_table_for_blog_property',
                                    python_callable=blogsSiteMetricsSummaryTable.run_site_metrics_update,
                                    dag=dag)

update_blog_metrics_by_landing_page_table = PythonOperator(task_id='create_blog_summary_table_by_landing_page',
                                    python_callable=blogsSiteMetricsByLandingPageSummaryTable.run_site_metrics_update,
                                    dag=dag)


#5. Set up Dependencies
#retrieve_s3_file >> upload_to_BQ_table
