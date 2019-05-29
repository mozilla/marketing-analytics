# This job pulls mozilla.org site related data and aggregates into one table
# Final dataset used to power experiment sizing and reporting of performance by landing page

import os
from google.cloud import bigquery
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')

# Set environment variable to authenticate using service account
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gkaberere/Google Drive/Github/gkLocalAppsServiceAccount.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json'

def calc_last_load_date(dataset_id, table_name):
    '''
    Finds the last date loaded into table by table_suffix
    :param dataset_id: Name of dataset
    :param table_name: Name of table
    :return last_load_date: the last table suffix of the table_name
    '''

    # Set the query
    client = bigquery.Client(project='ga-mozilla-org-prod-001')
    job_config = bigquery.QueryJobConfig()
    sql = f"""
    SELECT
    FORMAT_DATE('%Y%m%d', max(date)) AS last_load_date
    FROM
    `ga-mozilla-org-prod-001.{dataset_id}.{table_name}_*`
    """
    # Run the query
    read_query = client.query(
        sql,
        location='US',
        job_config=job_config)  # API request - starts the query
    #  Assign last date to last_load_date variable
    for row in read_query:
        return row.last_load_date
    return


def load_new_data(dataset_id, table_name, next_load_date, end_load_date):
    '''
    Queries different snippet related GA properties and loads results to a permanent table in bigquery
    :param dataset_id: Name of dataset to be loaded into
    :param table_name: Name of table to be loaded into
    :param next_load_date: Earliest date to be loaded into table_name
    :param end_load_date: Latest date to be loaded into table_name
    :return:
    '''
    while next_load_date < end_load_date:
        # Set dates required for loading new data
        next_load_date = datetime.strftime(next_load_date, '%Y%m%d')
        logging.info(f'Starting load for next load date: {next_load_date}')
        client = bigquery.Client(project='ga-mozilla-org-prod-001')
        load_dataset_id = dataset_id
        load_table_name = table_name
        load_table_suffix = next_load_date
        load_table_id = f'{load_table_name.lower()}_{load_table_suffix}'

        # Configure load job
        dataset_ref = client.dataset(load_dataset_id)
        table_ref = dataset_ref.table(load_table_id)
        load_job_config = bigquery.QueryJobConfig()  # load job call
        load_job_config.schema = [
            bigquery.SchemaField('date', 'DATE'),
            bigquery.SchemaField('country', 'STRING'),
            bigquery.SchemaField('source', 'STRING'),
            bigquery.SchemaField('medium', 'STRING'),
            bigquery.SchemaField('campaign', 'STRING'),
            bigquery.SchemaField('content', 'STRING'),
            bigquery.SchemaField('sessions', 'INTEGER'),
            bigquery.SchemaField('nonFXSessions', 'INTEGER'),
            bigquery.SchemaField('downloads', 'INTEGER'),
            bigquery.SchemaField('nonFXDownloads', 'INTEGER'),
            bigquery.SchemaField('countryCleaned', 'STRING')
        ]  # Define schema
        load_job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field='date',
        )
        load_job_config.write_disposition = 'WRITE_APPEND'  # Options are WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        load_job_config.destination = table_ref
        sql = f"""
        WITH parameters as(
        SELECT
          '{load_table_suffix}' as startDate,
          '{load_table_suffix}' as endDate
        ),
                
        sessionsTable as(SELECT
          date,
          language,
          landingPage,
          pagePathLevel1,
          pageName,
          visitIdentifier,
          country,
          source,
          medium,
          campaign,
          adContent,
          browser,
          SUM(entrances) as sessions,
          SUM(CASE WHEN browser != 'Firefox' THEN entrances ELSE 0 END) as nonFXSessions
        FROM (
          SELECT
            date,
            device.language,
            hits.page.pagePath as landingPage,
            hits.page.pagePathLevel1 as pagePathLevel1,
            CONCAT('/',REPLACE(hits.page.pagePathLevel2,'/',''),'/',REPLACE(hits.page.pagePathLevel3,'/',''),'/') as pageName,
            CONCAT(CAST(fullVisitorId AS string),CAST(visitId AS string)) as visitIdentifier,
            geoNetwork.country,
            trafficSource.source,
            trafficSource.medium,
            trafficSource.campaign,
            trafficSource.adContent,
            device.browser,
            CASE WHEN hits.isEntrance IS NOT NULL THEN 1 ELSE 0 END as entrances
          FROM `ga-mozilla-org-prod-001.65789850.ga_sessions_*`, UNNEST (hits) as hits
          WHERE
            _TABLE_SUFFIX >= (SELECT startDate FROM parameters)
            AND _TABLE_SUFFIX <= (SELECT endDate FROM parameters))
        GROUP BY date, language, landingPage, pagePathLevel1, pageName, visitIdentifier, country, source, medium, campaign, adContent, browser),
        
        downloadsTable as(
        SELECT
          date,
          language,
          visitIdentifier,
          country,
          source,
          medium,
          campaign,
          adContent,
          browser,
          SUM(IF(downloads > 0,1,0)) as downloads,
          SUM(IF(downloads > 0 AND browser != 'Firefox',1,0)) as nonFXDownloads
        FROM (SELECT
          date AS date,
          CONCAT(CAST(fullVisitorId AS string),CAST(visitId AS string)) as visitIdentifier,
          device.language,
          geoNetwork.country,
          trafficSource.source,
          trafficSource.medium,
          trafficSource.campaign,
          trafficSource.adContent,
          device.browser as browser,
          SUM(IF (hits.eventInfo.eventAction = "Firefox Download",1,0)) as downloads
        FROM
          `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
          UNNEST (hits) AS hits
        WHERE
          _TABLE_SUFFIX >= (SELECT startDate FROM parameters)
          AND _TABLE_SUFFIX <= (SELECT endDate FROM parameters)
          AND hits.type = 'EVENT'
          AND hits.eventInfo.eventCategory IS NOT NULL
          AND hits.eventInfo.eventLabel LIKE "Firefox for Desktop%"
        GROUP BY
          date,visitIdentifier, language, country, source, medium, campaign, adContent, browser)
        GROUP BY date, language, visitIdentifier, country, source, medium, campaign, adContent, browser),
        
        joinedTable as (
        SELECT
          sessionsTable.date,
          sessionsTable.visitIdentifier,
          sessionsTable.language,
          sessionsTable.landingPage,
          sessionsTable.pagePathLevel1,
          sessionsTable.pageName,
          sessionsTable.country,
          sessionsTable.source,
          sessionsTable.medium,
          sessionsTable.campaign,
          sessionsTable.adContent,
          sessionsTable.browser,
          sessionsTable.sessions,
          sessionsTable.nonFXSessions,
          downloadsTable.downloads,
          downloadsTable.nonFxDownloads
        FROM
          sessionsTable
        FULL JOIN
          downloadsTable
        ON
          sessionsTable.date = downloadsTable.date
          AND sessionsTable.visitIdentifier = downloadsTable.visitIdentifier
        WHERE sessionsTable.sessions != 0)
        
        SELECT
        PARSE_DATE('%Y%m%d', date) as date,
        language,
        landingPage,
        pagePathLevel1,
        pageName,
        country,
        source,
        medium,
        campaign,
        adContent,
        browser,
        SUM(sessions) as sessions,
        SUM(nonFXSessions) as nonFXSessions,
        SUM(downloads) as downloads,
        SUM(nonFXDownloads) as nonFXDownloads
        FROM joinedTable
        GROUP BY
          date, language, landingPage, pagePathLevel1, pageName, country, source, medium, campaign, adContent, browser
        """

        # Run Load Job
        query_job = client.query(
            sql,
            # Location must match that of the dataset(s) referenced in the query
            # and of the destination table.
            location='US',
            job_config=load_job_config)  # API request - starts the query

        query_job.result()  # Waits for the query to finish
        logging.info(f'Query results loaded to table {table_ref.path}')

        # Set next_load_date
        next_load_date = datetime.strptime(next_load_date, '%Y%m%d') + timedelta(1)
    return


def run_site_metrics_landing_page_update():
    # Find the last date when data was loaded into the table
    read_dataset_id = 'desktop'
    read_table_name = 'website_landing_page_metrics'
    last_load_date = calc_last_load_date(read_dataset_id, read_table_name)

    # Set dates required for loading new data
    last_load_date = datetime.strptime(last_load_date, '%Y%m%d')
    end_load_date = datetime.now() - timedelta(1)  # prior day to ensure data collection is complete
    next_load_date = last_load_date + timedelta(1)

    # Load most recent data
    load_dataset_id = read_dataset_id
    load_table_name = read_table_name
    load_new_data(load_dataset_id, load_table_name, next_load_date, end_load_date)
    return


if __name__ == '__main__':
    run_site_metrics_update()


