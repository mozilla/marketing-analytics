# This job pulls desktop related metrics data from the  telemetry GCP Project and saves summarized copy
# in the Marketing GCP Project for use in dashboarding, reporting and adhoc analysis
# Sources are telemetry

import os
from google.cloud import bigquery
from datetime import datetime, timedelta, date
import pandas as pd
import logging
import json
import urllib

job_name = 'telemetry_desktop_usage_metrics_retrieve'
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')

# Set environment variables
# TODO: Remove local file reference prior to pushing to app engine
os.environ['desktop_environment_variables'] = 'desktopTelemetryEnvVariables.json'
#os.environ['desktop_environment_variables'] = '/Users/gkaberere/Google Drive/Github/marketing-analytics/bqQueries/' \
#                                              'desktopGrowth/reporting/desktopTelemetryEnvVariables.json'

# TODO: Change path from local to environment file
with open('desktopTelemetryEnvVariables.json') as json_file:
    variables = json.load(json_file)
    marketing_project_var = variables['marketing_project']
    telemetry_project_var = variables['telemetry_project']

current_date = date.today()
seven_days_ago = current_date - timedelta(7)


def calc_last_load_date(project, dataset_id, table_name):
    '''
    Finds the last date loaded into table by table_suffix so as to set starting point for next days pull
    :param project: Name of project
    :param dataset_id: Name of dataset
    :param table_name: Name of table
    :return last_load_date: the last table suffix of the table_name
    '''

    # TODO: Change path from local to environment variable
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'
    #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gkaberere/Google Drive/Github/marketing-analytics' \
    #                                               '/AppEngine-moz-mktg-prod-001/moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'

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


def calc_max_data_availability(project, dataset_id, table_name, date_field):
    '''
        Finds the last date loaded into table by table_suffix so as to set end date for pull
        :param project: Name of project
        :param dataset_id: Name of dataset
        :param table_name: Name of table
        :return last_load_date: the last table suffix of the table_name
        '''

    # TODO: Change path from local to environment variable
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'moz-fx-data-derived-datasets-marketing-analytics.json'
    #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gkaberere/Google Drive/Github/marketing-analytics' \
    #                                               '/AppEngine-moz-mktg-prod-001/moz-fx-data-derived-datasets-marketing-analytics.json'

    # Set the query
    client = bigquery.Client(project=project)
    job_config = bigquery.QueryJobConfig()
    sql = f"""
        SELECT
            MAX({date_field}) AS max_date
        FROM
            `{project}.{dataset_id}.{table_name}`
        WHERE
        {date_field} > DATE("{seven_days_ago}")
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
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'moz-fx-data-derived-datasets-marketing-analytics.json'
    #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gkaberere/Google Drive/Github/marketing-analytics' \
    #                                               '/AppEngine-moz-mktg-prod-001/moz-fx-data-derived-datasets-marketing-analytics.json'

    # next_load_date = datetime.strftime(next_load_date, '%Y%m%d')
    logging.info(f'{job_name}: Starting read for next load date: {next_load_date}')
    client = bigquery.Client(project=read_project)
    sql = f"""
            WITH usageData as(
                SELECT
                submission_date AS submission,
                CASE WHEN country IS NULL THEN '' ELSE country END AS country,
                CASE WHEN source IS NULL THEN '' ELSE source END AS source,
                CASE WHEN medium IS NULL THEN '' ELSE medium END AS medium,
                CASE WHEN campaign IS NULL THEN '' ELSE campaign END AS campaign,
                CASE WHEN content IS NULL THEN '' ELSE content END AS content,
                CASE WHEN distribution_id IS NULL THEN '' ELSE distribution_id END AS distribution_id,
                CASE
                  WHEN source IS NULL AND medium IS NULL AND campaign IS NULL AND content IS NULL AND distribution_id IS NULL THEN 'darkFunnel'
                  ELSE CASE
                  WHEN source IS NULL AND medium IS NULL AND campaign IS NULL AND content IS NULL AND distribution_id IS NOT NULL THEN 'partnerships'
                  ELSE 'mozFunnel'
                END END AS funnelOrigin,
                SUM(dau) AS dau,
                SUM(wau) AS wau,
                SUM(mau) AS mau
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
                  distribution_id,
                  funnelOrigin),

                installData as (
                SELECT
                  submission,
                  CASE WHEN metadata.geo_country IS NULL THEN '' ELSE metadata.geo_country END as country,
                  CASE WHEN environment.settings.attribution.source IS NULL THEN 'unknown' ELSE environment.settings.attribution.source END as source,
                  CASE WHEN environment.settings.attribution.medium IS NULL THEN 'unknown' ELSE environment.settings.attribution.medium END as medium,
                  CASE WHEN environment.settings.attribution.campaign IS NULL THEN 'unknown' ELSE environment.settings.attribution.campaign END as campaign,
                  CASE WHEN environment.settings.attribution.content IS NULL THEN 'unknown' ELSE environment.settings.attribution.content END as content,
                  CASE WHEN environment.partner.distribution_id IS NULL THEN '' ELSE environment.partner.distribution_id END as distribution_id,
                  CASE
                    WHEN environment.settings.attribution.source IS NULL AND environment.settings.attribution.medium IS NULL 
                        AND environment.settings.attribution.campaign IS NULL AND environment.settings.attribution.content IS NULL 
                        AND environment.partner.distribution_id IS NULL THEN 'darkFunnel'
                    ELSE CASE
                    WHEN environment.settings.attribution.source IS NULL AND environment.settings.attribution.medium IS NULL 
                        AND environment.settings.attribution.campaign IS NULL AND environment.settings.attribution.content IS NULL 
                        AND environment.partner.distribution_id IS NOT NULL THEN 'partnerships'
                    ELSE 'mozFunnel'
                  END END AS funnelOrigin,
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
                  distribution_id,
                  funnelOrigin),

                joinedKeyMetrics as (
                SELECT
                  usageData.submission,
                  installData.submission as installSubmission,
                  usageData.country,
                  installData.country as installCountry,
                  usageData.source,
                  installData.source as installSource,
                  usageData.medium,
                  installData.medium as installMedium,
                  usageData.campaign,
                  installData.campaign as installCampaign,
                  usageData.content,
                  installData.content as installContent,
                  usageData.distribution_id,
                  installData.distribution_id as installDistributionID,
                  usageData.funnelOrigin,
                  installData.funnelOrigin as installFunnelOrigin,
                  usageData.dau,
                  usageData.wau,
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
                  AND usageData.distribution_id = installData.distribution_id
                  AND usageData.funnelOrigin = installData.funnelOrigin),

                desktopKeyMetrics as (
                SELECT
                  joinedKeyMetrics.submission,
                  joinedKeyMetrics.country,
                  joinedKeyMetrics.source,
                  joinedKeyMetrics.medium,
                  joinedKeyMetrics.campaign,
                  joinedKeyMetrics.content,
                  joinedKeyMetrics.distribution_id,
                  joinedKeyMetrics.funnelOrigin,
                  joinedKeyMetrics.dau,
                  joinedKeyMetrics.wau,
                  joinedKeyMetrics.mau,
                  joinedKeyMetrics.installs
                FROM
                  joinedKeyMetrics
                WHERE
                  joinedKeyMetrics.submission IS NOT NULL

                UNION ALL

                SELECT
                  joinedKeyMetrics.installSubmission,
                  joinedKeyMetrics.installCountry,
                  joinedKeyMetrics.installSource,
                  joinedKeyMetrics.installMedium,
                  joinedKeyMetrics.installCampaign,
                  joinedKeyMetrics.installContent,
                  joinedKeyMetrics.installDistributionID,
                  joinedKeyMetrics.installFunnelOrigin,
                  0 as dau,
                  0 as wau,
                  0 as mau,
                  joinedKeyMetrics.installs
                FROM
                  joinedKeyMetrics
                WHERE
                  joinedKeyMetrics.submission IS NULL)

                SELECT * FROM desktopKeyMetrics
            """
    desktop_usage_data_df = client.query(sql).to_dataframe()
    logging.info(f'{job_name}: Read complete from telemetry desktop metrics for {next_load_date}')
    return desktop_usage_data_df


def clean_up_schema(data_df):
    '''
    :param data_df: Pandas dataframe for query operation
    :return: data_df with adjusted schema
    '''

    logging.info(f'{job_name}: running clean_up_schema')
    # Change Nulls to 0 to enable schema cleanup
    data_df['installs'].fillna(0, inplace=True)

    # Convert floats to integer and clean up schema
    data_df['submission'] = data_df.submission
    data_df['country'] = data_df.country.astype(str)
    data_df['source'] = data_df.source.astype(str)
    data_df['medium'] = data_df.medium.astype(str)
    data_df['campaign'] = data_df.campaign.astype(str)
    data_df['content'] = data_df.content.astype(str)
    data_df['distribution_id'] = data_df.distribution_id.astype(str)
    data_df['funnelOrigin'] = data_df.funnelOrigin.astype(str)
    data_df['dau'] = data_df.dau.astype(int)
    data_df['wau'] = data_df.wau.astype(int)
    data_df['mau'] = data_df.mau.astype(int)
    data_df['installs'] = data_df.installs.astype(int)
    logging.info(f'{job_name}: clean_up_schema successfully completed')
    return data_df


def remove_url_encoding(data_df):
    '''
    :param data_df: Pandas dataframe from query operation after clean_up_schema completed
    :return: data_df with additional columns of cleaned up attribution fields
    '''

    logging.info(f'{job_name}: Remove_url_encoding fields function starting')

    # Remove URL Encoding from attribution fields - duplicate columns to preserve original telemetry columns
    data_df['sourceCleaned'] = list(
        map(lambda data_df: urllib.parse.unquote_plus(urllib.parse.unquote_plus(data_df, encoding='utf-8')),
            data_df['source']))
    data_df['mediumCleaned'] = list(
        map(lambda data_df: urllib.parse.unquote_plus(urllib.parse.unquote_plus(data_df, encoding='utf-8')),
            data_df['medium']))
    data_df['campaignCleaned'] = list(
        map(lambda data_df: urllib.parse.unquote_plus(urllib.parse.unquote_plus(data_df, encoding='utf-8')),
            data_df['campaign']))
    data_df['contentCleaned'] = list(
        map(lambda data_df: urllib.parse.unquote_plus(urllib.parse.unquote_plus(data_df, encoding='utf-8')),
            data_df['content']))
    logging.info(f'{job_name}: Remove_url_encoding - URL encoding successfully completed')
    return data_df


def mediumCleanup(data_df):
    # Cleanup direct traffic to equal (none)
    if data_df['source'] == 'www.mozilla.org' and data_df['medium'] == '%2528none%2529':
        return '(none)'
    # Cleanup organic traffic medium to organic
    elif 'www.google' in data_df['source'] and data_df['medium'] == 'referral' \
            and (data_df['campaign'] == '%2528not%2Bset%2529' or data_df['content'] == '%2528not%2Bset%2529'):
        return 'organic'
    elif 'bing.com' in data_df['source'] and data_df['medium'] == 'referral' \
            and (data_df['campaign'] == '%2528not%2Bset%2529' or data_df['content'] == '%2528not%2Bset%2529'):
        return 'organic'
    elif 'yandex' in data_df['source'] and data_df['medium'] == 'referral' \
            and (data_df['campaign'] == '%2528not%2Bset%2529' or data_df['content'] == '%2528not%2Bset%2529'):
        return 'organic'
    elif 'search.yahoo' in data_df['source'] and data_df['medium'] == 'referral' \
            and (data_df['campaign'] == '%2528not%2Bset%2529' or data_df['content'] == '%2528not%2Bset%2529'):
        return 'organic'
    elif 'search.seznam' in data_df['source'] and data_df['medium'] == 'referral' \
            and (data_df['campaign'] == '%2528not%2Bset%2529' or data_df['content'] == '%2528not%2Bset%2529'):
        return 'organic'
    else:
        return data_df['mediumCleaned']


def sourceCleanup(data_df):
    # Cleanup direct traffic to equal (none)
    if data_df['source'] == 'www.mozilla.org' and data_df['medium'] == '%2528none%2529':
        return '(direct)'
    # Cleanup organic traffic sources
    elif 'www.google' in data_df['source'] and data_df['medium'] == 'referral' \
            and (data_df['campaign'] == '%2528not%2Bset%2529' or data_df['content'] == '%2528not%2Bset%2529'):
        return 'google'
    elif 'bing.com' in data_df['source'] and data_df['medium'] == 'referral' \
            and (data_df['campaign'] == '%2528not%2Bset%2529' or data_df['content'] == '%2528not%2Bset%2529'):
        return 'bing'
    elif 'yandex' in data_df['source'] and data_df['medium'] == 'referral' \
            and (data_df['campaign'] == '%2528not%2Bset%2529' or data_df['content'] == '%2528not%2Bset%2529'):
        return 'yandex'
    elif 'search.yahoo' in data_df['source'] and data_df['medium'] == 'referral' \
            and (data_df['campaign'] == '%2528not%2Bset%2529' or data_df['content'] == '%2528not%2Bset%2529'):
        return 'yahoo'
    elif 'search.seznam' in data_df['source'] and data_df['medium'] == 'referral' \
            and (data_df['campaign'] == '%2528not%2Bset%2529' or data_df['content'] == '%2528not%2Bset%2529'):
        return 'seznam'
    else:
        return data_df['sourceCleaned']


def redefine_source_medium(data_df):
    '''
    Redefine telemetry source and medium fields so as to enable joining with top of funnel GA data
    :param data_df: pandas dataframe from query after add_acquisition_funnel has been completed
    :return: data_df with additional columns sourceCleaned and mediumCleaned
    '''

    logging.info(f'{job_name}: Redefine_source_medium transformation starting - cleaning mediums')
    data_df['mediumCleaned'] = data_df.apply(mediumCleanup, axis=1)

    logging.info(f'{job_name}: Redefine_source_medium transformation - cleaning sources')
    data_df['sourceCleaned'] = data_df.apply(sourceCleanup, axis=1)

    return data_df


def standardize_country_names(data_df, marketing_project):
    '''
    Creates new column country name with full country name to make it easier to query and analyze the data
   :param data_df: data frame after source and medium transformations
   :param marketing_project: Bigquery project where standardized country lookup table is saved
   :return:
   '''

    # Change credentials to telemetry credentials
    # TODO: Change path from local to environment variable
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'
    #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gkaberere/Google Drive/Github/marketing-analytics' \
    #                                               '/AppEngine-moz-mktg-prod-001/moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'

    # Read standardized country table and load to data frame
    logging.info(f'{job_name}: Starting standardize_country_names country clean up')
    client = bigquery.Client(project=marketing_project)
    sql = f"""
            SELECT
            *
            FROM
            `ga-mozilla-org-prod-001.lookupTables.standardizedCountryList`
            """
    standardized_country_list_df = client.query(sql).to_dataframe()

    # Change country columns to lower case to enable join
    data_df['country'] = list(map(str.lower, data_df.country))
    standardized_country_list_df['rawCountry'] = list(map(str.lower, standardized_country_list_df.rawCountry))

    # Join dataframes
    data_df = pd.merge(data_df, standardized_country_list_df, left_on='country', right_on='rawCountry', how='left')

    # Drop unnecessary columns and rename new column
    data_df = data_df.drop('rawCountry', axis=1)
    data_df = data_df.rename(index=str, columns={'standardizedCountry': 'countryName'})
    data_df['countryName'] = data_df.countryName.astype(str)

    # Return country column to upper case
    data_df['country'] = list(map(str.upper, data_df.country))

    logging.info(f'{job_name}: Standardize_country_names country cleanup completed')
    return data_df


def load_desktop_usage_data(data, load_project, load_dataset_id, load_table_name, next_load_date):
    '''
    Function saves dataframe = data in temporary csv then loads it into bq so as to give more control on the load job configuration
    :param data: data frame after adding standardized country name column
    :param load_project: Bigquery project to load table into
    :param load_dataset_id: Bigquery dataset to load data into
    :param load_table_name: Bigquery table to load data into
    :param next_load_date: the date of the data to be loaded. Also serves as table suffix.
    :return:
    '''

    # Change credentials to marketing analytics credentials
    # TODO: Change path from local to environment variable
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'
    #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gkaberere/Google Drive/Github/marketing-analytics' \
    #                                               '/AppEngine-moz-mktg-prod-001/moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'

    # Set dates required for loading new data
    next_load_date = datetime.strftime(next_load_date, '%Y%m%d')
    logging.info(f'{job_name}: load_desktop_usage_data - Starting load for next load date: {next_load_date}')
    load_table_name = f'{load_table_name.lower()}_{next_load_date}'

    client = bigquery.Client(project=load_project)

    # Configure Load Job
    datasetRef = client.dataset(load_dataset_id)  # create a dataset reference using a chosen dataset ID
    table_ref = datasetRef.table(load_table_name)  # create a table reference using a chosen table ID
    job_config = bigquery.LoadJobConfig()  # load job call
    job_config.schema = [
        bigquery.SchemaField('submission', 'DATE'),
        bigquery.SchemaField('country', 'STRING'),
        bigquery.SchemaField('source', 'STRING'),
        bigquery.SchemaField('medium', 'STRING'),
        bigquery.SchemaField('campaign', 'STRING'),
        bigquery.SchemaField('content', 'STRING'),
        bigquery.SchemaField('distribution_id', 'STRING'),
        bigquery.SchemaField('funnelOrigin', 'STRING'),
        bigquery.SchemaField('dau', 'INTEGER'),
        bigquery.SchemaField('wau', 'INTEGER'),
        bigquery.SchemaField('mau', 'INTEGER'),
        bigquery.SchemaField('installs', 'INTEGER'),
        bigquery.SchemaField('sourceCleaned', 'STRING'),
        bigquery.SchemaField('mediumCleaned', 'STRING'),
        bigquery.SchemaField('campaignCleaned', 'STRING'),
        bigquery.SchemaField('contentCleaned', 'STRING'),
        bigquery.SchemaField('countryName', 'STRING')
    ]  # Define schema
    job_config.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field='submission')
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.max_bad_records = 10  # number of bad records allowed before job fails

    # Run Load Job
    job = client.load_table_from_dataframe(
        data,
        table_ref,
        location='US',  # Must match the destination dataset location.
        job_config=job_config)  # API request

    job.result()  # Waits for table load to complete.
    logging.info(f'{job_name}: load_desktop_usage_data - Loaded {job.output_rows} rows into {table_ref.path}')
    return None


def run_desktop_telemetry_retrieve():
    # Find the last date when data was loaded into the table
    load_project = marketing_project_var
    load_dataset_id = 'desktop'
    load_table_name = 'desktop_corp_metrics'
    last_load_date = calc_last_load_date(load_project, load_dataset_id, load_table_name)

    # Find the most recent data that data is available
    read_project = telemetry_project_var
    read_dataset_id = 'telemetry'
    read_table_name_1 = 'firefox_desktop_exact_mau28_by_dimensions_v1'
    read_table_name_2 = 'telemetry_new_profile_parquet_v2'
    end_load_date_1 = calc_max_data_availability(read_project, read_dataset_id, read_table_name_1, 'submission_date')
    end_load_date_2 = calc_max_data_availability(read_project, read_dataset_id, read_table_name_2, 'submission')
    end_load_date = min(end_load_date_1,
                        end_load_date_2)  # set end_load_date to min date between two tables to avoid partial loads
    logging.info(f'{job_name}: Loading data up to and including {end_load_date}')

    # Set dates required for loading new data
    last_load_date = datetime.strptime(last_load_date, "%Y%m%d")
    # next_load_date = date(2018, 1, 8)
    # end_load_date = date(2018, 5, 25)
    # ToDO: When ready to run automatically remove manually set lines above
    next_load_date = last_load_date + timedelta(1)
    next_load_date = datetime.date(next_load_date)

    while next_load_date <= end_load_date:
        data = read_desktop_usage_data(read_project, next_load_date)
        data = clean_up_schema(data)
        data = remove_url_encoding(data)
        data = redefine_source_medium(data)
        data = standardize_country_names(data, load_project)
        load_desktop_usage_data(data, load_project, load_dataset_id, load_table_name, next_load_date)

        # Set next load date
        next_load_date = next_load_date + timedelta(1)
    return


if __name__ == '__main__':
    run_desktop_telemetry_retrieve()

