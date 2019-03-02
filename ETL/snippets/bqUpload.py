# Create a table and loading data from a local file
import os
from datetime import datetime
from google.cloud import bigquery

# Set environment variable to authenticate using service account
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gkaberere/Google Drive/Github/gkLocalAppsServiceAccount.json'

client = bigquery.Client()
fileName = '/Users/gkaberere/spark-warehouse/testSnippet/metaData/snippets_metadata_20190227.csv'
datasetID = 'snippets'
tableID = f'snippets_metadata'


datasetRef = client.dataset(datasetID) # create a dataset reference using a chosen dataset ID
table_ref = datasetRef.table(tableID) # create a table reference using a chosen table ID
load_job_config = bigquery.LoadJobConfig() # load job call
load_job_config.source_format = bigquery.SourceFormat.CSV
load_job_config.schema = [
    bigquery.SchemaField('id', 'STRING'),
    bigquery.SchemaField('name', 'STRING'),
    bigquery.SchemaField('campaign', 'STRING'),
    bigquery.SchemaField('category', 'STRING'),
    bigquery.SchemaField('url', 'STRING'),
    bigquery.SchemaField('body', 'STRING')
]
load_job_config.quote_character = '"'
load_job_config.skip_leading_rows = 0
load_job_config.max_bad_records = 0 # number of bad records allowed before job fails
load_job_config.write_disposition = 'WRITE_TRUNCATE' #Options are WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY see doc

with open(fileName, 'rb') as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location='US',  # Must match the destination dataset location.
        job_config=load_job_config)  # API request

job.result()  # Waits for table load to complete.

print('Loaded {} rows into {}:{}.'.format(job.output_rows, datasetID, tableID))

