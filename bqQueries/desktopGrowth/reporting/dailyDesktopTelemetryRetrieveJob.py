# This job pulls desktop related metrics data from the  telemetry GCP Project and saves summarized copy
# in the Marketing GCP Project for use in dashboarding, reporting and adhoc analysis
# Dataset is an interim dataset that will be cleaned, rationalized and joined with google analytics
# Sources are telemetry

import os
from google.cloud import bigquery
from datetime import datetime, timedelta, date

# Set environment variables
#TODO: Remove local file reference prior to pushing to app engine
#TODO: Remember to move desktop environment variableto app engine as well
#os.environ['desktop_environment_variables'] = 'desktopTelemetryEnvVariables.json'
os.environ['desktop_environment_variables'] = '/Users/gkaberere/Google Drive/Github/marketing-analytics/bqQueries/' \
                                              'desktopGrowth/reporting/desktopTelemetryEnvVariables.json'

#TODO: Change path from local to environment file
with open('/Users/gkaberere/Google Drive/Github/marketing-analytics/bqQueries/'
          'desktopGrowth/reporting/desktopTelemetryEnvVariables.json') as json_file:
    variables = json.load(json_file)
    marketing_project_var = variables['marketing_project']
    telemetry_project_var = variables['telemetry_project']

current_date = date.today()
seven_days_ago = current_date-timedelta(7)

#TODO: Add project as a parameter once tested
def calc_last_load_date(project,dataset_id, table_name):
    '''
    Finds the last date loaded into table by table_suffix so as to set starting point for next days pull
    :param project: Name of project
    :param dataset_id: Name of dataset
    :param table_name: Name of table
    :return last_load_date: the last table suffix of the table_name
    '''

    #TODO: Change path from local to environment variable
    #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gkaberere/Google Drive/Github/marketing-analytics' \
                                                   '/App Engine - moz-mktg-prod-001/moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'

    # Set the query
    client = bigquery.Client(project=project)
    job_config = bigquery.QueryJobConfig()
    sql = f"""
    SELECT
    max(_table_suffix) AS last_load_date
    FROM
    `{project}.{dataset_id}.{table_name}_*`
    """
    # Run the query
    read_query = client.query(
        sql,
        location='US',
        job_config=job_config)  # API request - starts the query
    #  Assign last date to last_load_date variable
    for row in read_query:
        logging.info(f'{job_name}: Last load into {table_name} was for {row.last_load_date}')
        return row.last_load_date
    return None


def calc_max_data_availability(project, dataset_id, table_name):
    '''
        Finds the last date loaded into table by table_suffix so as to set end date for pull
        :param project: Name of project
        :param dataset_id: Name of dataset
        :param table_name: Name of table
        :return last_load_date: the last table suffix of the table_name
        '''

    # TODO: Change path from local to environment variable
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gkaberere/Google Drive/Github/marketing-analytics' \
                                                   '/App Engine - moz-mktg-prod-001/moz-fx-data-derived-datasets-marketing-analytics.json'

    # Set the query
    client = bigquery.Client(project=project)
    job_config = bigquery.QueryJobConfig()
    sql = f"""
        SELECT
            MAX(submission_date) AS max_date
        FROM
            `{project}.{dataset_id}.{table_name}`
        WHERE
        submission_date > DATE("{seven_days_ago}")
        """
    # Run the query
    read_query = client.query(
        sql,
        location='US',
        job_config=job_config)  # API request - starts the query
    #  Assign last date to last_load_date variable
    for row in read_query:
        logging.info(f'{job_name}: Most recent available data in {table_name} is for {row.max_date}')
        return row.max_date
    return None


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
                  usageData.dau,
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
                LIMIT 100
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
    load_job_config = bigquery.QueryJobConfig()  # load job call
    load_job_config.schema = [
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
    load_job_config.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field='submission',
    )
    load_job_config.write_disposition = 'WRITE_TRUNCATE'  # Options are WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    load_job_config.destination = table_ref

    # Run Load Job
    query_job = client.query(
        data,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        location='US',
        job_config=load_job_config)  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    logging.info(f'{job_name}: Query results loaded to table {table_ref.path}')

    return None

def run_desktop_telemetry_retrieve():
    # Find the last date when data was loaded into the table
    load_project = marketing_project_var
    load_dataset_id = 'telemetry'
    load_table_name = 'desktop_corp_metrics'
    last_load_date = calc_last_load_date(load_project,load_dataset_id, load_table_name) #remove comments before sending to app engine

    # Find the most recent data that data is available
    read_project = telemetry_project_var
    read_dataset_id_1 = 'analysis'
    read_dataset_id_2 = 'telemetry'
    read_table_name_1 = 'firefox_desktop_exact_mau28_by_dimensions_v1'
    read_table_name_2 = 'telemetry_new_profile_parquet_v2'
    end_load_date_1 = calc_max_data_availability(read_project, read_dataset_id_1, read_table_name_1)
    end_load_date_2 = calc_max_data_availability(read_project, read_dataset_id_2, read_table_name_2)
    end_load_date = min(end_load_date_1, end_load_date_2)

    # Set dates required for loading new data
    last_load_date = datetime.strptime(last_load_date, "%Y%m%d")
    next_load_date = last_load_date + timedelta(1)

    while next_load_date < end_load_date:



    return

if __name__ == '__main__':
    run_desktop_telemetry_retrieve()
