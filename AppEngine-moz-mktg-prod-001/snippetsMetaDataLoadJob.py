import logging
import os
import urllib3
import tempfile
import shutil
import json
from google.cloud import storage, bigquery
from datetime import datetime
import certifi

# Start Logging
job_name = 'snippets_metadata_load_job'
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')

# Set environment variable for authentication and data retrieve
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f"""{os.environ['variables_path']}moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json"""
os.environ['snippets_environment_variables'] = f"""{os.environ['variables_path']}snippetsEnvVariables.json"""

with open('snippetsEnvVariables.json') as json_file:
    variables = json.load(json_file)
    bucket_var = variables['bucket']
    metadata_url_var = variables['metadata_url']
    project_var = variables['project']
    gcs_file_name_var = variables['gcs_metadata_folder']

# Determine file date for retrieve
current_date = datetime.now()
file_date = datetime.strftime(current_date, '%Y%m%d')

url = f'''{metadata_url_var}snippets_metadata_{file_date}.csv'''
bucket = bucket_var
blobname = f'metaData/snippets_metadata_{file_date}.csv'
dataset_id = 'snippets'
table_name = 'snippets_metadata'
gcs_file_name = f'{gcs_file_name_var}/snippets_metadata_{file_date}.csv'



def download_metadata_file(url, temp_dir):
    logging.info(f'{job_name}: Starting file request from S3 for {file_date}')
    csvfile = os.path.join(temp_dir, f'snippets_metadata_{file_date}.csv')
    with open(csvfile, 'wb') as file:
        http = urllib3.PoolManager(
            cert_reqs='CERT_REQUIRED',
            ca_certs=certifi.where()
        )
        response = http.request('GET', url)
        response.status # use for logging
        file.write(response.data)
    logging.info(f'{job_name}: File for {file_date} found')
    return csvfile


def upload_to_gcs(csvfile, bucket, blobname):
    logging.info(f'{job_name}: Starting load for {file_date} to google cloud storage')
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    #bucket = bucket
    blob = bucket.blob(blobname)
    blob.upload_from_filename(filename=csvfile)
    gcslocation = f'gs://{bucket}/{blobname}'
    logging.info(f'{job_name}: file successfully uploaded {gcslocation} ...')
    return gcslocation


def ingest(url, bucket, blobname):
    try:
        temp_dir = tempfile.mkdtemp(prefix='snippet_metadata')
        file_csv = download_metadata_file(url, temp_dir)
        return upload_to_gcs(file_csv, bucket, blobname)
    finally:
        logging.debug(f'Cleaning up - removing {temp_dir}')
        shutil.rmtree(temp_dir)


def bq_metadata_upload(gcs_file_name, dataset_id, table_name):
    logging.info(f'{job_name}: Starting load of {file_date} to bigquery')

    #Configure load job
    client = bigquery.Client(project=f'{project_var}')
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)
    load_job_config = bigquery.LoadJobConfig()
    load_job_config.source_format = bigquery.SourceFormat.CSV
    load_job_config.schema = [
        bigquery.SchemaField('id', 'STRING'),
        bigquery.SchemaField('name', 'STRING'),
        bigquery.SchemaField('campaign', 'STRING'),
        bigquery.SchemaField('category', 'STRING'),
        bigquery.SchemaField('url', 'STRING'),
        bigquery.SchemaField('body', 'STRING'),
        bigquery.SchemaField('tags', 'STRING')
    ]
    load_job_config.quote_character = '"'
    load_job_config.allow_quoted_newlines = True
    load_job_config.skip_leading_rows = 0
    load_job_config.max_bad_records = 0
    load_job_config.write_disposition = 'WRITE_TRUNCATE'

    job = client.load_table_from_uri(
        gcs_file_name,
        table_ref,
        location='US',  # Must match the destination dataset location.
        job_config=load_job_config)

    job.result()
    logging.info('{}: Loaded file {} with {} rows into {}:{}.'.format(file_date, job_name, job.output_rows, dataset_id, table_name))
    return

def run_snippets_metadata_load_job():
    ingest(url, bucket, blobname)
    bq_metadata_upload(gcs_file_name, dataset_id, table_name)

if __name__ == '__main__':
    run_snippets_metadata_load_job()


# Things to reverse to get it to run
# 1 removed variable reference
# 2 in app.py instead of calling it from main made it call like snippet performance - also moved variables up to variables section