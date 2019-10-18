#Leanplum API Documentation can be found here: https://docs.leanplum.com/docs/exporting-raw-data-via-the-api
#https://docs.leanplum.com/reference#ab-tests

import urllib3
import re
import json
import ndjson
import time
import os
import tempfile
import certifi
import logging
from google.cloud import bigquery
import pandas as pd
import csv
import itertools
from flatten_json import flatten

# Start Logging
job_name = 'leanplum_load_job'
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')


# Set Environment Variables and constants
os.environ['config'] = f"""{os.environ['leanplum_config_file_location']}leanplum_variables_config.json"""

# Set variables
config = os.environ['config']
with open(config, 'r') as config_file:
    variables = json.load(config_file)

    ANDROID_APP_ID_KEY = variables["ANDROID_APP_ID_KEY"]
    ANDROID_EXPORT_KEY = variables["ANDROID_APP_ID_KEY"]
    ANDROID_CONTENT_READ_ONLY_KEY = variables["ANDROID_APP_ID_KEY"]
    IOS_APP_ID_KEY = variables["ANDROID_APP_ID_KEY"]
    IOS_EXPORT_KEY = variables["ANDROID_APP_ID_KEY"]
    IOS_CONTENT_READ_ONLY_KEY = variables["ANDROID_APP_ID_KEY"]

# API URLs
getABTests_api = f'''https://www.leanplum.com/api?action=getAbTests'''
exportData_api = f'''https://www.leanplum.com/api?action=exportData'''


# function gets passed the device type, selects the appropriate keys to use in getting the data
def get_ab_test_information(deviceType):
    # Select authentication keys for App
    if deviceType.upper() == 'IOS':
        APP_ID = IOS_APP_ID_KEY
        EXPORT_KEY = IOS_EXPORT_KEY
        CONTENT_KEY = IOS_CONTENT_READ_ONLY_KEY
    elif deviceType.upper() == 'ANDROID':
        APP_ID = ANDROID_APP_ID_KEY
        EXPORT_KEY = ANDROID_EXPORT_KEY
        CONTENT_KEY = ANDROID_CONTENT_READ_ONLY_KEY
    else:
        #TODO: How do you make sure that if this happens, airflow knows it was a failed job?
        logging.ERROR(f'{job_name} - get_ab_test_information: Device not one of IOS or Android. Stopping Application')
        return 'Device not one of IOS or Android'

    # Construct abTests URL
    getABTests_url = getABTests_api+f'''&appId={APP_ID}&clientKey={CONTENT_KEY}&apiVersion=1.0.6'''

    # Get AB Tests Information from API
    logging.info(f'''{job_name} - get_ab_test_information: Requesting list of A/B Tests from API for {deviceType}''')
    http = urllib3.PoolManager(
        cert_reqs='CERT_REQUIRED',
        ca_certs=certifi.where()
    )
    response = http.request('GET', getABTests_url)
    response_return = response.data.decode('utf-8')
    response_json = json.loads(response_return)

    # Check response for successful data pull
    logging.info(f'''{job_name} - get_ab_test_information: Checking response for successful data retrieval''')

    if response_json['response'][0]['success'] == True and response.status == 200:
        logging.info(f'''{job_name} - get_ab_test_information: Data successfully retrieved''')
        data = response_json['response'][0]

        json_file = '/Users/gkaberere/spark-warehouse/leanPlum/saved_json_ndjson.json'

        with open(json_file, 'w') as file:
            ndjson.dump(data, file)


    else:
        #TODO: How do you make sure that if this happens airflow knows it's a failed job
        logging.error(f'''{job_name} - get_ab_test_information: Response did not meet success = True or have a 
        response status = 200''')
        return

    return

deviceType = 'ios'
get_ab_test_information(deviceType)