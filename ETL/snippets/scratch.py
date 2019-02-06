import logging
import json
import csv
import requests
import io
import os
from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas as pd


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')

# Set environment variables for authentication
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'
#TODO: Remove path for environment variable before deploying
os.environ['redashAPIKey'] = '/Users/gkaberere/Google Drive/Github/marketing-analytics/ETL/snippets/snippetsEnvVariables.json'


# Set constants required for job
with open('/Users/gkaberere/Google Drive/Github/marketing-analytics/ETL/snippets/snippetsEnvVariables.json', 'r') as api_file:
    REDASH_API_URL = json.load(api_file)['redash_api']

with open('/Users/gkaberere/Google Drive/Github/marketing-analytics/ETL/snippets/snippetsEnvVariables.json', 'r') as api_file:
    GCP_BUCKET = json.load(api_file)['gcp_bucket']




# 1. Retrieve file from redash
def retrieve_csv_from_redash():
    logging.info("Starting retrieve_csv_from_redash")
    redash_query_results = requests.get(REDASH_API_URL).content
    redash_dataFrame = pd.read_csv(io.StringIO(redash_query_results.decode('utf-8')))
    logging.info("retrieve_csv_from_redash successfully completed")
    return redash_dataFrame

#2. Check that the file is for the right date
current_date = datetime.now()






data = retrieve_csv_from_redash()
print(data.head())



#logging.info("Starting retrieve_csv_from_redash")
#redash_query_results = requests.get(REDASH_API_URL).content
#redash_dataFrame = pd.read_csv(io.StringIO(redash_query_results.decode('utf-8')))
#logging.info("retrieve_csv_from_redash successfully completed")
#print(redash_dataFrame.head())



# 2. Check that the file is for the right date
#current_date = datetime.now()
#redash_dataFrame = retrieve_csv_from_redash()
#print(redash_dataFrame.head())


# 3. Save csv file in Google Cloud Bucket



# 4. Load data into bigquery snippets_telemetry_tracking table




#def write

#logging.info("starting job - ")
#redash_query_results = requests.get(REDASH_API_URL).content
#df = pd.read_csv(io.StringIO(redash_query_results.decode('utf-8')))

#file = '/Users/gkaberere/spark-warehouse/testSnippet/snippetsData/etlTest20190122.csv'
#with open(file, 'w') as file:
#    columnNames = ('date', 'message_id', 'release_channel', 'country_code', 'os', 'version', 'impressions', 'clicks', 'blocks')
#    df.to_csv(file, index=False, header=True, encoding='utf-8')
#print('transformed file completed')





#with requests.session() as session:
 #   redash_query_results = session.get(redash_api_url)
  #  decoded_redash_query_results = redash_query_results.content.decode('utf-8')
   # print(decoded_redash_query_results)




#    rawData = pd.read_csv(io.StringIO(urlData.decode('utf-8')))





#redash_query_results = requests.get(redash_api).content
#print(redash_query_results)





#import csv
#import requests

#CSV_URL = 'http://samplecsvs.s3.amazonaws.com/Sacramentorealestatetransactions.csv'


#with requests.Session() as s:
#    download = s.get(CSV_URL)

#    decoded_content = download.content.decode('utf-8')

#    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
#    my_list = list(cr)
#    for row in my_list:
#        print(row)