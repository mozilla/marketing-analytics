import logging
import csv
import requests
import io
import os
from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas as pd

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')

# Set environment variable to authenticate using service account
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'

# Set constants required for job
REDASH_API_URL = "https://sql.telemetry.mozilla.org/api/queries/60856/results.csv?api_key=gIXykstcSKJR0nMSeMnvG4NBpt91UVkLaZYqqWvL"
GCP_BUCKET = "gs://snippets-data-transfer/daily-tracking-data/"
dataset_id = "snippets"
table_name = "snippets_telemetry_tracking"

# 1. Retrieve file from redash

# 2. Load file to BigQuery
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')

# Set environment variable to authenticate using service account
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'

def calc_last_load_date(dataset_id, table_name):
    '''
    Finds the last date loaded into table by table_suffix
    :param dataset_id: Name of dataset
    :param table_name: Name of table
    :return last_load_date: the last table suffix of the table_name
    '''

    # Set the query
    client = bigquery.Client(project='ga-mozilla-org-prod-001')
    job_config = bigquery.QueryJobConfig()
    sql = f"""
    SELECT
    max(_table_suffix) AS last_load_date
    FROM
    `ga-mozilla-org-prod-001.{dataset_id}.{table_name}_*`
    """
    # Run the query
    read_query = client.query(
        sql,
        location='US',
        job_config=job_config)  # API request - starts the query
    #  Assign last date to last_load_date variable
    for row in read_query:
        return row.last_load_date

def load_new_telemetry_snippet_data(dataset_id, table_name, next_load_date, end_load_date):
    '''
    Reads csv file from google cloud storage bucket and loads it into bigquery
    :param dataset_id: Name of dataset to be loaded into
    :param table_name: Name of table to be loaded into
    :param next_load_date: Earliest date to be loaded into table_name
    :param end_load_date: Latest date to be loaded into table_name
    :return:
    '''
    while next_load_date < end_load_date:
        # Set dates required for loading new data
        next_load_date = datetime.strftime(next_load_date, '%Y%m%d')
        logging.info(f'snippetTelemetryDailyRetrieve: Starting load for next load date: {next_load_date}')
        client = bigquery.Client(project='ga-mozilla-org-prod-001')
        file = f'gs://snippets-data-transfer/daily-tracking-data/snippets_{next_load_date}.csv'
        load_dataset_id = dataset_id
        load_table_name = table_name
        load_table_suffix = next_load_date
        load_table_id = f'{load_table_name.lower()}_{load_table_suffix}'

        # Configure load job
        dataset_ref = client.dataset(load_dataset_id)
        table_ref = dataset_ref.table(load_table_id)
        load_job_config = bigquery.LoadJobConfig()  # load job call
        load_job_config.schema = [
            bigquery.SchemaField('sendDate', 'DATE'),
            bigquery.SchemaField('messageID', 'STRING'),
            bigquery.SchemaField('releaseChannel', 'STRING'),
            bigquery.SchemaField('locale', 'STRING'),
            bigquery.SchemaField('countryCode', 'STRING'),
            bigquery.SchemaField('os', 'STRING'),
            bigquery.SchemaField('version', 'STRING'),
            bigquery.SchemaField('impressions', 'INTEGER'),
            bigquery.SchemaField('clicks', 'INTEGER'),
            bigquery.SchemaField('blocks', 'INTEGER')
        ] # Define schema
        load_job_config.source_format = bigquery.SourceFormat.CSV
        load_job_config.skip_leading_rows = 1
        load_job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field='sendDate',
        )
        #load_job_config.max_bad_records = 5
        load_job_config.write_disposition = 'WRITE_APPEND'  # Options are WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY

        load_job = client.load_table_from_uri(
                file,
                table_ref,
                location='US',
                job_config=load_job_config
            )

        assert load_job.job_type == 'load'

        load_job.result() # Waits for the query to finish
        logging.info(f'snippetTelemetryDailyRetrieve: File {file} loaded to table {table_ref.path}')

        # Set next_load_date
        next_load_date = datetime.strptime(next_load_date, '%Y%m%d') + timedelta(1)

def run_snippets_telemetry_update():
    # Find the last date when data was loaded into the table
    read_dataset_id = 'snippets'
    read_table_name = 'snippets_telemetry_tracking'
    last_load_date = calc_last_load_date(read_dataset_id, read_table_name)

    # Set dates required for loading new data
    last_load_date = datetime.strptime(last_load_date, '%Y%m%d')
    end_load_date = datetime.now() - timedelta(1)  # prior day to ensure data collection is complete
    next_load_date = last_load_date + timedelta(1)

    # Load most recent data
    load_dataset_id = read_dataset_id
    load_table_name = read_table_name
    load_new_telemetry_snippet_data(load_dataset_id, load_table_name, next_load_date, end_load_date)
    return


if __name__ == '__main__':
    run_snippets_telemetry_update()
