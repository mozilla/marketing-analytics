import os
from google.cloud import bigquery
from datetime import datetime, timedelta, date
import logging
import json

job_name = 'telemetry_desktop_usage_metrics_retrieve'
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')

def read_desktop_usage_data(read_project, next_load_date):
    '''
    Retrieves a specific dataset for loading into a permanent table in bigquery and stores in a pandas dataframe
    :param read_project: Project that has dataset
    :param read_dataset_id: Dataset that has table to be read
    :param read_table_name: Name of table to be read
    :param next_load_date: Desired date
    :return: data for loading into table
    '''

    # Change credentials to telemetry credentials
    # TODO: Change path from local to environment variable
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gkaberere/Google Drive/Github/marketing-analytics' \
                                                   '/App Engine - moz-mktg-prod-001/moz-fx-data-derived-datasets-marketing-analytics.json'

    #next_load_date = datetime.strftime(next_load_date, '%Y%m%d')
    logging.info(f'{job_name}: Starting read for next load date: {next_load_date}')
    client = bigquery.Client(project=read_project)
    sql = f"""
            WITH usageData as(
                SELECT
                  submission_date as submission,
                  CASE WHEN country IS NULL THEN '' ELSE country END as country,
                  CASE WHEN source IS NULL THEN '' ELSE source END as source,
                  CASE WHEN medium IS NULL THEN '' ELSE medium END as medium,
                  CASE WHEN campaign IS NULL THEN '' ELSE campaign END as campaign,
                  CASE WHEN content IS NULL THEN '' ELSE content END as content,
                  CASE WHEN distribution_id IS NULL THEN '' ELSE distribution_id END as distribution_id,
                  SUM(dau) as dau,
                  SUM(mau) as mau
                FROM `{read_project}.analysis.firefox_desktop_exact_mau28_by_dimensions_v1`
                WHERE
                  submission_date = DATE("{next_load_date}")
                GROUP BY
                  submission,
                  country,
                  source,
                  medium,
                  campaign,
                  content,
                  distribution_id),

                installData as (
                SELECT
                  submission,
                  CASE WHEN metadata.geo_country IS NULL THEN '' ELSE metadata.geo_country END as country,
                  CASE WHEN environment.settings.attribution.source IS NULL THEN '' ELSE environment.settings.attribution.source END as source,
                  CASE WHEN environment.settings.attribution.medium IS NULL THEN '' ELSE environment.settings.attribution.medium END as medium,
                  CASE WHEN environment.settings.attribution.campaign IS NULL THEN '' ELSE environment.settings.attribution.campaign END as campaign,
                  CASE WHEN environment.settings.attribution.content IS NULL THEN '' ELSE environment.settings.attribution.content END as content,
                  CASE WHEN environment.partner.distribution_id IS NULL THEN '' ELSE environment.partner.distribution_id END as distribution_id,
                  COUNT(DISTINCT client_id) as installs
                FROM
                  `{read_project}.telemetry.telemetry_new_profile_parquet_v2`
                WHERE
                  submission = DATE("{next_load_date}")
                GROUP BY
                  submission,
                  country,
                  source,
                  medium,
                  campaign,
                  content,
                  distribution_id),

                desktopKeyMetrics as (
                SELECT
                  usageData.submission,
                  usageData.country,
                  usageData.source,
                  usageData.medium,
                  usageData.campaign,
                  usageData.content,
                  usageData.distribution_id,
                  CAST(usageData.dau as INT64) as dau,
                  usageData.mau,
                  installData.installs
                FROM
                  usageData
                FULL JOIN
                  installData
                ON
                  usageData.submission = installData.submission
                  AND usageData.country = installData.country
                  AND usageData.source = installData.source
                  AND usageData.medium = installData.medium
                  AND usageData.campaign = installData.campaign
                  AND usageData.content = installData.content
                  AND usageData.distribution_id = installData.distribution_id)

                SELECT * FROM desktopKeyMetrics
            """
    desktop_usage_data_df = client.query(sql).to_dataframe()
    logging.info(f'{job_name}: Read complete from telemetry desktop metrics for {next_load_date}')
    return desktop_usage_data_df


def load_desktop_usage_data(data, load_project, load_dataset_id, load_table_name, next_load_date):
    '''
    Loads dataset previously read into a permanent bigquery table
    :param load_project: Project to write dataset to
    :param load_dataset_id: Dataset that has table to be read
    :param read_table_name: Name of table to be read
    :param next_load_date: Desired date
    :return: data for loading into table
        '''

    # Change credentials to marketing analytics credentials
    # TODO: Change path from local to environment variable
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gkaberere/Google Drive/Github/marketing-analytics' \
                                                   '/App Engine - moz-mktg-prod-001/moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'

    # Set dates required for loading new data
    next_load_date = datetime.strftime(next_load_date, '%Y%m%d')
    logging.info(f'{job_name}: Starting load for next load date: {next_load_date}')
    client = bigquery.Client(project=load_project)
    load_dataset_id = load_dataset_id
    load_table_name = load_table_name
    load_table_suffix = next_load_date
    load_table_id = f'{load_table_name.lower()}_{load_table_suffix}'

    # Configure load job
    dataset_ref = client.dataset(load_dataset_id)
    table_ref = dataset_ref.table(load_table_id)
    job_config = bigquery.QueryJobConfig()  # load job call
    job_config.schema = [
        bigquery.SchemaField('submission', 'DATE'),
        bigquery.SchemaField('country', 'STRING'),
        bigquery.SchemaField('source', 'STRING'),
        bigquery.SchemaField('medium', 'STRING'),
        bigquery.SchemaField('campaign', 'STRING'),
        bigquery.SchemaField('content', 'STRING'),
        bigquery.SchemaField('distribution_id', 'STRING'),
        bigquery.SchemaField('dau', 'INTEGER'),
        bigquery.SchemaField('mau', 'INTEGER'),
        bigquery.SchemaField('installs', 'INTEGER')
    ]  # Define schema
    job_config.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field='submission',
    )
    job_config.write_disposition = 'WRITE_TRUNCATE'  # Options are WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY

    # Run Load Job
    query_job = client.load_table_from_dataframe(
        dataframe=data,
        destination=table_ref,
        location='US')

    query_job.result()  # Waits for the query to finish
    logging.info(f'{job_name}: Query results loaded to table {table_ref.path}')

    return None




next_load_date = date(2018,1,1)
end_load_date = date(2018,1,2)
data = read_desktop_usage_data('moz-fx-data-derived-datasets', next_load_date)
print(data)

load_project = "ga-mozilla-org-prod-001"
load_dataset_id = 'testDataSet2'
load_table_name = 'desktop_corp_metrics'

load_desktop_usage_data(data,load_project, load_dataset_id, load_table_name, next_load_date )


#load_project = 'ga-mozilla-org-prod-001'
#load_dataset_id = 'testDataSet2'
#load_table_name = 'desktop_corp_metrics'

#load_desktop_usage_data(data, load_project, load_dataset_id, load_table_name, next_load_date)



#temp_dir = tempfile.mkdtemp(prefix='telemetry_read')
#    csv_file = os.path.join(temp_dir, f'desktop_usage_{next_load_date}.csv')




    #for row in read_query:  # API request - fetches results
        # Row values can be accessed by field name or index
     #   assert row[0] == row.submission == row['submission']
      #  assert row[1] == row.country == row['country']
       # assert row[2] == row.source == row['source']
        #assert row[3] == row.medium == row['medium']
        #ssert row[4] == row.campaign == row['campaign']
        #assert row[5] == row.content == row['content']
        #assert row[6] == row.distribution_id == row['distribution_id']
        #assert row[7] == row.dau == row['dau']
        #assert row[8] == row.mau == row['mau']
        #assert row[9] == row.installs == row['installs']


        #print(row)