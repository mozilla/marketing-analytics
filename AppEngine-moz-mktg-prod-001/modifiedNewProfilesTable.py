## This script modifies the new profile pings to remove the effect of new profile pings from the profiles-per-install
# change shipped with FX67 starting May 19th

from google.cloud import bigquery
from datetime import datetime, timedelta, date
import os
import logging
import json

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')
job_name = 'adjusting_new_profiles'

# TODO: Set it up to first check if google_application_credentials for the user are set, if not, then use the service account
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f"""{os.environ['variables_path']}moz-fx-data-derived-datasets-marketing-analytics.json"""
os.environ['config'] = f"""{os.environ['variables_path']}desktop_telemetry_env_variables.json"""

config = os.environ['config']

with open(config, 'r') as json_file:
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


def load_adjusted_new_profiles(next_load_date, max_date_pull):
    client = bigquery.Client()
    datasetID = 'analysis'
    tableName = 'gkabbz_newProfileCountModifications'

    logging.info(f'{job_name}: load_adjusted_new_profiles - Starting load for next load date: {next_load_date}')
    next_load_date_string = datetime.strftime(next_load_date, '%Y%m%d')
    tableID = f'''{tableName}_{next_load_date_string}'''


    datasetRef = client.dataset(datasetID)
    tableRef = datasetRef.table(tableID)
    job_config = bigquery.QueryJobConfig() #load job call
    job_config.write_disposition = 'WRITE_TRUNCATE' #Options are WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    job_config.destination = tableRef
    sql = f"""
        WITH newProfiles as (SELECT
          submission,
          client_id,
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
          END END AS funnelOrigin
        FROM
          `moz-fx-data-derived-datasets.telemetry.telemetry_new_profile_parquet_v2`
            WHERE
              submission = "{next_load_date}"
            GROUP BY 1,2,3,4,5,6,7,8,9),
            
        firstShutdown as (
        SELECT
          submission_date_s3,
          'firstShutdown' as tableOrigin,
          client_id as fs_client_id,
          scalar_parent_startup_profile_selection_reason as profileSelectionReason
        FROM
          `moz-fx-data-derived-datasets.telemetry.first_shutdown_summary_v4`
        WHERE
          submission_date_s3 >= "{next_load_date}"
          AND submission_date_s3 <= "{max_date_pull}"
        GROUP BY 1,2,3,4
        ),
        
        mainSummary as (
        SELECT
          submission_date_s3,
          'mainSummary' as tableOrigin,
          client_id as ms_client_id,
          scalar_parent_startup_profile_selection_reason as profileSelectionReason
        FROM
          `moz-fx-data-derived-datasets.telemetry.main_summary_v4` 
        WHERE
          submission_date_s3 >= "{next_load_date}"
          AND submission_date_s3 <= "{max_date_pull}"
        GROUP BY 1,2,3,4
        ),
        
        mainPingsJoin as (
        SELECT
          submission_date_s3,
          tableOrigin,
          fs_client_id,
          CASE 
            WHEN profileSelectionReason IN ('restart-skipped-default', 'firstrun-skipped-default') THEN CONCAT('01-',profileSelectionReason) ELSE 
            CASE WHEN profileSelectionReason IS NULL THEN 'zz-null' ELSE profileSelectionReason 
            END END as profileSelectionReason
        FROM 
          firstShutDown
        
        UNION ALL
        SELECT
          submission_date_s3,
          tableOrigin,
          ms_client_id,
          CASE 
            WHEN profileSelectionReason IN ('restart-skipped-default', 'firstrun-skipped-default') THEN CONCAT('01-',profileSelectionReason) ELSE 
            CASE WHEN profileSelectionReason IS NULL THEN 'zz-null' ELSE profileSelectionReason 
            END END as profileSelectionReason
        FROM
          mainSummary
        ),
        
        firstOccurenceMainPing as (
        SELECT
          submission_date_s3,
          tableOrigin,
          fs_client_id as joinClientID,
          profileSelectionReason,
          firstShow
        FROM (
          SELECT
            submission_date_s3,
            tableOrigin,
            fs_client_id,
            profileSelectionReason,
            ROW_NUMBER() OVER(PARTITION BY fs_client_id ORDER BY submission_date_s3, profileSelectionReason, tableOrigin) as firstShow
          FROM
            mainPingsJoin)
        WHERE firstShow = 1
        ),
        
        joined as (
        SELECT
          *
        FROM
          newProfiles
        LEFT JOIN
          firstOccurenceMainPing
        ON
          newProfiles.client_id = firstOccurenceMainPing.joinClientID)
        
        SELECT
          submission,
          client_id,
          country,
          source,
          medium,
          campaign,
          content,
          distribution_id,
          funnelOrigin,
          tableOrigin,
          profileSelectionReason
        FROM joined
        WHERE profileSelectionReason NOT LIKE '%-skipped-default'
    """
    job_config.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field='submission')


    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        location='US',
        job_config=job_config)  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    logging.info(f'{job_name}: load_adjusted_new_profiles - Load completed for: {next_load_date}')
    return None

def run_adjusted_new_profiles():
    # Find the last date when data was loaded into the table
    load_project = telemetry_project_var
    load_dataset_id = 'analysis'
    load_table_name = 'gkabbz_newProfileCountModifications'
    last_load_date = calc_last_load_date(load_project, load_dataset_id, load_table_name)

    # Find the most recent data that data is available
    read_project = telemetry_project_var
    read_dataset_id = 'telemetry'
    read_table_name_1 = 'telemetry_new_profile_parquet_v2'
    end_load_date = calc_max_data_availability(read_project, read_dataset_id, read_table_name_1, 'submission_date')

    logging.info(f'{job_name}: Loading data up to and including {end_load_date}')

    # Set Dates
    last_load_date = datetime.strptime(last_load_date, "%Y%m%d")
    next_load_date = last_load_date + timedelta(1)
    next_load_date = datetime.date(next_load_date)

    #next_load_date = date(2019, 10, 24)
    #end_load_date = date(2019, 10, 24)

    while next_load_date <= end_load_date:
        max_date_pull = next_load_date + timedelta(1)
        load_adjusted_new_profiles(next_load_date, max_date_pull)

        # Set next dates
        next_load_date = next_load_date + timedelta(1)
    return


if __name__ == '__main__':
    run_adjusted_new_profiles()









