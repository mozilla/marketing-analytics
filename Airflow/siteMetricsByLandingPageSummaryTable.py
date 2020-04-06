# This job pulls mozilla.org site related data and aggregates into one table
# Final dataset used to power experiment sizing and reporting of performance by landing page

import os
from google.cloud import bigquery
from datetime import datetime, timedelta, date
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')

# Set environment variable to authenticate using service account
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f"""{os.environ['variables_path']}moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json"""
current_date = date.today()
seven_days_ago = current_date - timedelta(7)


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
    max(date) AS last_load_date
    FROM
    `ga-mozilla-org-prod-001.{dataset_id}.{table_name}`
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
    while next_load_date <= end_load_date:
        # Set dates required for loading new data
        next_load_date = datetime.strftime(next_load_date, '%Y%m%d')
        logging.info(f'Starting load for next load date: {next_load_date}')
        client = bigquery.Client(project='ga-mozilla-org-prod-001')
        load_dataset_id = dataset_id
        load_table_name = table_name
        load_table_id = f'{load_table_name.lower()}'

        # Configure load job
        dataset_ref = client.dataset(load_dataset_id)
        table_ref = dataset_ref.table(load_table_id)
        load_job_config = bigquery.QueryJobConfig()  # load job call
        load_job_config.schema = [
            bigquery.SchemaField('date', 'DATE'),
            bigquery.SchemaField('site', 'STRING'),
            bigquery.SchemaField('device_category', 'STRING'),
            bigquery.SchemaField('operating_system', 'STRING'),
            bigquery.SchemaField('language', 'STRING'),
            bigquery.SchemaField('landing_page', 'STRING'),
            bigquery.SchemaField('locale', 'STRING'),
            bigquery.SchemaField('page_name', 'STRING'),
            bigquery.SchemaField('page_level_1', 'STRING'),
            bigquery.SchemaField('page_level_2', 'STRING'),
            bigquery.SchemaField('page_level_3', 'STRING'),
            bigquery.SchemaField('page_level_4', 'STRING'),
            bigquery.SchemaField('page_level_5', 'STRING'),
            bigquery.SchemaField('country', 'STRING'),
            bigquery.SchemaField('source', 'STRING'),
            bigquery.SchemaField('medium', 'STRING'),
            bigquery.SchemaField('campaign', 'STRING'),
            bigquery.SchemaField('ad_content', 'STRING'),
            bigquery.SchemaField('sessions', 'INTEGER'),
            bigquery.SchemaField('non_FX_Sessions', 'INTEGER'),
            bigquery.SchemaField('downloads', 'INTEGER'),
            bigquery.SchemaField('non_FX_Downloads', 'INTEGER'),
            bigquery.SchemaField('pageviews', 'INTEGER'),
            bigquery.SchemaField('unique_pageviews', 'INTEGER'),
            bigquery.SchemaField('single_page_sessions', 'INTEGER'),
            bigquery.SchemaField('bounces', 'INTEGER'),
            bigquery.SchemaField('exits', 'INTEGER')
        ]  # Define schema
        load_job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field='date',
        )
        load_job_config.clustering_fields = ['page_name', 'country', 'locale', 'medium']
        load_job_config.write_disposition = 'WRITE_APPEND'  # Options are WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        load_job_config.destination = table_ref
        sql = f"""
        WITH parameters as(
        SELECT
          '{next_load_date}' as startDate,
          '{next_load_date}' as endDate
        ),
                
        sessionsTable as(
        -- Pull sessions related activity by landing page
        SELECT
          date,
          deviceCategory,
          operatingSystem,
          language,
          landingPage,
          locale,
          page_level_1,
          page_level_2,
          page_level_3,
          page_level_4,
          page_level_5,
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
            device.deviceCategory,
            device.operatingSystem,
            device.language,
            hits.page.pagePath as landingPage,
            hits.page.pagePathLevel1 as locale,
            -- splitting the pagePath to make it easier to filter on pages in dashboards
            SPLIT(split(hits.page.pagePath, '?')[offset(0)], '/')[SAFE_offset(2)] as page_level_1,
            SPLIT(split(hits.page.pagePath, '?')[offset(0)], '/')[SAFE_offset(3)] as page_level_2,
            SPLIT(split(hits.page.pagePath, '?')[offset(0)], '/')[SAFE_offset(4)] as page_level_3,
            SPLIT(split(hits.page.pagePath, '?')[offset(0)], '/')[SAFE_offset(5)] as page_level_4,
            SPLIT(split(hits.page.pagePath, '?')[offset(0)], '/')[SAFE_offset(6)] as page_level_5,
            CONCAT(CAST(fullVisitorId AS string),CAST(visitId AS string)) as visitIdentifier,
            geoNetwork.country,
            trafficSource.source,
            trafficSource.medium,
            trafficSource.campaign,
            trafficSource.adContent,
            device.browser,
            CASE WHEN hits.isEntrance IS NOT NULL THEN 1 ELSE 0 END as entrances
          FROM 
            `ga-mozilla-org-prod-001.65789850.ga_sessions_*`, 
            UNNEST (hits) as hits
          WHERE
            _TABLE_SUFFIX >= (SELECT startDate FROM parameters)
            AND _TABLE_SUFFIX <= (SELECT endDate FROM parameters))
        GROUP BY 
          date, deviceCategory, operatingSystem, language, landingPage, locale, page_level_1, page_level_2, page_level_3, page_level_4, page_level_5, 
          visitIdentifier, country, source, medium, campaign, adContent, browser),
        
        downloadsTable as(
        SELECT
          date,
          deviceCategory,
          operatingSystem,
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
          device.deviceCategory,
          device.operatingSystem,
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
          date,visitIdentifier, deviceCategory, operatingSystem, language, country, source, medium, campaign, adContent, browser)
        GROUP BY date, deviceCategory, operatingSystem, language, visitIdentifier, country, source, medium, campaign, adContent, browser),
        
        
        pageviewsTable as (
        -- Select pageview metrics by visit identifier
        SELECT
          date,
          CONCAT(CAST(fullVisitorId AS string),CAST(visitId AS string)) as visitIdentifier,
          COUNT(DISTINCT(hits.page.pagePath)) AS uniquePageviews,
          COUNT(*) as pageviews
        FROM
          `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
          UNNEST (hits) as hits
        WHERE
          _TABLE_SUFFIX >= (SELECT startDate FROM parameters)
          AND _TABLE_SUFFIX <= (SELECT endDate FROM parameters)
          AND hits.type='PAGE'
        GROUP BY 
          1,2),
        
        bouncesTable as (
        -- Select bounces specific metrics by visit identifier
        SELECT
          date,
          visitIdentifier,
          SUM(CASE WHEN hitNumber = first_interaction THEN visits ELSE 0 END) as singlePageSessions,
          SUM(CASE WHEN hitNumber = first_interaction THEN bounces ELSE 0 END) as bounces,
          SUM(exits) as exits
        FROM (
        SELECT
          date,
          CONCAT(CAST(fullVisitorId AS string),CAST(visitId AS string)) as visitIdentifier,
          visitStartTime,
          hits.page.pagePath,
          totals.visits,
          totals.bounces,
          hits.hitNumber,
          MIN(IF(hits.isInteraction IS NOT NULL, hits.hitNumber, 0)) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_interaction,
          CASE WHEN hits.isExit IS NOT NULL THEN 1 ELSE 0 END as exits
        FROM
          `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
          UNNEST (hits) as hits
        WHERE
          _TABLE_SUFFIX >= (SELECT startDate FROM parameters)
          AND _TABLE_SUFFIX <= (SELECT endDate FROM parameters))
        GROUP BY 
          1,2),
        
        joinedTable as (
        -- Join tables based off of date and visitIdentifier
        SELECT
          sessionsTable.date,
          sessionsTable.visitIdentifier,
          sessionsTable.deviceCategory,
          sessionsTable.operatingSystem,
          sessionsTable.language,
          sessionsTable.landingPage,
          sessionsTable.locale,
          sessionsTable.page_level_1,
          sessionsTable.page_level_2,
          sessionsTable.page_level_3,
          sessionsTable.page_level_4,
          sessionsTable.page_level_5,
          sessionsTable.country,
          sessionsTable.source,
          sessionsTable.medium,
          sessionsTable.campaign,
          sessionsTable.adContent,
          sessionsTable.browser,
          sessionsTable.sessions,
          sessionsTable.nonFXSessions,
          pageViewsTable.pageviews,
          downloadsTable.downloads,
          downloadsTable.nonFxDownloads,
          pageviewsTable.uniquePageViews,
          bouncesTable.singlePageSessions,
          bouncesTable.bounces,
          bouncesTable.exits
        FROM
          sessionsTable
        FULL JOIN
          downloadsTable
        ON
          sessionsTable.date = downloadsTable.date
          AND sessionsTable.visitIdentifier = downloadsTable.visitIdentifier
        FULL JOIN
          pageviewsTable
        ON
          sessionsTable.date = pageViewsTable.date
          AND sessionsTable.visitIdentifier = pageViewsTable.visitIdentifier
        FULL JOIN
          bouncesTable
        ON
          sessionsTable.date = bouncesTable.date
          AND sessionsTable.visitIdentifier = bouncesTable.visitIdentifier
        -- To minimize table size, filtering for sessions != 0
        WHERE 
          sessionsTable.sessions != 0)
        
        
        SELECT
          PARSE_DATE('%Y%m%d', date) as date,
          -- Adding a site field incase we want to append the blog traffic to this same table for YoY comparability
          'mozilla.org' as site,
          deviceCategory as device_category,
          operatingSystem as operating_system,
          language,
          landingPage as landing_page,
          locale,
          -- Creating a pagename field that is similar to how pages are referred to internally
          CASE 
            WHEN page_level_5 IS NOT NULL OR page_level_5 != ' ' 
              THEN CONCAT('/', page_level_1, '/', page_level_2, '/', page_level_3, '/', page_level_4, '/', page_level_5)
            WHEN page_level_4 IS NOT NULL OR page_level_4 != ' '
              THEN CONCAT('/', page_level_1, '/', page_level_2, '/', page_level_3, '/', page_level_4)
            WHEN page_level_3 IS NOT NULL OR page_level_3 != ' ' 
              THEN CONCAT('/', page_level_1, '/', page_level_2, '/', page_level_3)
            WHEN page_level_2 IS NOT NULL OR page_level_2 != ' ' 
              THEN CONCAT('/', page_level_1, '/', page_level_2)
            ELSE
              CONCAT('/', page_level_1, '/')
            END as page_name,
          page_level_1,
          page_level_2,
          page_level_3,
          page_level_4,
          page_level_5,
          country,
          source,
          medium,
          campaign,
          adContent as ad_content,
          browser,
          SUM(sessions) as sessions,
          SUM(nonFXSessions) as non_FX_Sessions,
          SUM(downloads) as downloads,
          SUM(nonFXDownloads) as non_FX_Downloads,
          SUM(pageviews) as pageviews,
          SUM(uniquePageviews) as unique_pageviews,
          SUM(singlePageSessions) as single_page_sessions,
          SUM(bounces) as bounces,
          SUM(exits) as exits
        FROM 
          joinedTable
        GROUP BY
          date, site, device_category, operating_system, language, landing_page, locale, page_name, page_level_1, page_level_2, page_level_3, 
          page_level_4, page_level_5, country, source, medium, campaign, ad_Content, browser
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
        next_load_date = datetime.date(next_load_date)
    return


def run_site_metrics_landing_page_update():
    # Find the last date when data was loaded into the table
    read_dataset_id = 'desktop'
    read_table_name = 'website_landing_page_metrics'
    last_load_date = calc_last_load_date(read_dataset_id, read_table_name)
    print("This is the last load date")
    print(last_load_date)


    # Set dates required for loading new data
    #last_load_date = datetime.strptime(last_load_date, '%Y%m%d')
    end_load_date = date.today() - timedelta(1)  # prior day to ensure data collection is complete
    next_load_date = last_load_date + timedelta(1)

    # Load most recent data
    load_dataset_id = read_dataset_id
    load_table_name = read_table_name
    load_new_data(load_dataset_id, load_table_name, next_load_date, end_load_date)
    return

def initialize_table():
    read_dataset_id = 'desktop'
    read_table_name = 'website_landing_page_metrics'
    current_date = datetime.now()
    current_date = datetime.date(current_date)

    next_load_date = current_date
    end_load_date = datetime.strptime("20200311", '%Y%m%d')
    end_load_date = datetime.date(end_load_date)

    load_new_data(read_dataset_id, read_table_name, next_load_date, end_load_date)



if __name__ == '__main__':
    #run_site_metrics_landing_page_update()
    initialize_table()


