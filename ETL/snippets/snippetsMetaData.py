import logging
import os
import urllib3
import tempfile
from google.cloud.storage import Blob
from google.cloud import storage
from datetime import datetime

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gkaberere/Google Drive/Github/gkLocalAppsServiceAccount.json'
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gkaberere/Google Drive/Github/marketing-analytics/App Engine - moz-mktg-prod-001/moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'
current_date = datetime.now()
file_date = datetime.strftime(current_date, '%Y%m%d')

permanent_dir = '/Users/gkaberere/spark-warehouse/testSnippet/metaData'
temp_dir = tempfile.mkdtemp(prefix='snippet_metadata')
temp_file_name = os.path.join(permanent_dir, f'snippets_metadata_{file_date}.csv') # Change to temp when needed
url = f'''https://s3-us-west-2.amazonaws.com/snippets-prod-us-west/metadata/my3zy7my7ca1pa9si1ta3ke7wu9ni7re4tu8zo8d/snippets_metadata_{file_date}.csv'''
bucket = 'snippets-data-transfer'
blobname = f'metaData/snippets_metadata_{file_date}.csv'
csv_file_location = f'/users/gkaberere/spark-warehouse/testSnippet/metaData/snippets_metadata_{file_date}.csv'


def download_metadata_file(url, temp_file_name):
    with open(temp_file_name, 'wb') as csvfile:
        http = urllib3.PoolManager()
        response = http.request('GET', url)
        response.status # use for logging
        csvfile.write(response.data)
    return csvfile


def upload_to_gcs(csvfile, bucket, blobname):
   client = storage.Client()
   bucket = client.get_bucket(bucket)
   blob = bucket.blob(blobname)
   blob.upload_from_filename(filename=csvfile)
   gcslocation = f'gs://{bucket}/{blobname}'
   logging.info(f'Uploaded {gcslocation} ...')
   return gcslocation

csvfile = download_metadata_file(url, temp_file_name)
upload_to_gcs(csv_file_location, bucket, blobname)


### upload to cloud storage works now next is to do the handoff using temporary file storage