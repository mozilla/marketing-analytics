# This job pulls desktop related metrics data from telemetry, cleans and prepares it for joining with google analytics acquisition data
# Dataset is an interim dataset that is joined with google analytics
# Sources are telemetry

import os
from google.cloud import bigquery
from datetime import datetime, timedelta
import logging
import pandas as pd

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')

# Set environment variable to authenticate using service account
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'

read_dataset_id = 'telemetry'
read_table_name = 'desktopTelemetryDrop'
start_load_date = '2017-01-01'
end_load_date = '2017-01-07'
project_id = 'ga-mozilla-org-prod-001'

sql = """
    SELECT
      *
    FROM
      `ga-mozilla-org-prod-001.telemetry.desktopTelemetryDrop`
"""

df = pd.read_gbq(sql, dialect='standard', project_id=project_id)
print(df.head())