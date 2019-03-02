import logging
import requests
import csv
import os
import urllib3
import tempfile
from google.cloud.storage import blob
from google.cloud import storage
import pandas as pd

date = '20190227f'
permanent_dir = '/Users/gkaberere/spark-warehouse/testSnippet/metaData'
temp_dir = tempfile.mkdtemp(prefix='snippet_metadata')
temp_file_name = os.path.join(permanent_dir, f'snippets_metadata_{date}.csv') # Change to temp when needed
url = f'''https://s3-us-west-2.amazonaws.com/snippets-prod-us-west/metadata/my3zy7my7ca1pa9si1ta3ke7wu9ni7re4tu8zo8d/snippets_metadata_20190227.csv'''


def download_metadata_file(url, temp_file_name):
    with open(temp_file_name, 'wb') as csvfile:
        http = urllib3.PoolManager()
        response = http.request('GET', url)
        response.status
        csvfile.write(response.data)
    return csvfile

download_metadata_file(url, temp_file_name)



#### Downloading file to local works. Next test uploading from local to Google Cloud storage


bucket = 'gs://snippets-data-transfer/'

def upload(file, bucket):
   client = storage.Client()
   bucket = client.get_bucket(bucket)
   blob = Blob(blobname, bucket)
   blob.upload_from_filename(csvfile)
   gcslocation = 'gs://snippets-data-transfer/metaData/'
   logging.info('Uploaded {} ...'.format(gcslocation))
   return gcslocation






bucket = 'gs://snippets-data-transfer/metaData/'
client = storage.Client()
bucket = client.get_bucket(bucket_name=bucket)
blob = Blob(blobname, bucket)








    def test_urllib3():
        import urllib3
        http = urllib3.PoolManager()
        response = http.request('GET', _URL)
        response.status


    return response.data



#import requests
#url = f'''https://s3-us-west-2.amazonaws.com/snippets-prod-us-west/metadata/my3zy7my7ca1pa9si1ta3ke7wu9ni7re4tu8zo8d/snippets_metadata_20190227.csv'''
#data = requests.get(url, verify=True).text
#print(data)




import requests
import csv
import os

temp_file_name = 'temp_csv.csv'
url = 'http://url.to/file.csv'
download = requests.get(url)

with open(temp_file_name, 'w') as temp_file:
    temp_file.writelines(download.content)

with open(temp_file_name, 'rU') as temp_file:
    csv_reader = csv.reader(temp_file, dialect=csv.excel_tab)
    for line in csv_reader:
        print line

# delete the temp file after process
os.remove(temp_file_name)



#bucket_url = "gs://snippets-data-transfer/metaData/"

#client = storage.Client('ga-mozilla-org-prod-001')
#bucket = client.get_bucket(bucket_url)
#blob = bucket.blob('test')
#blob.upload



#bucket = client.get_bucket(bucket_url)

#blob = bucket.get_blob(file_path)
#blob.upload_from_filename(filename=file_path)

