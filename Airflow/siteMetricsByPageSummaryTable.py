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
            bigquery.SchemaField('page', 'STRING'),
            bigquery.SchemaField('locale', 'STRING'),
            bigquery.SchemaField('page_level_1', 'STRING'),
            bigquery.SchemaField('page_level_2', 'STRING'),
            bigquery.SchemaField('page_level_3', 'STRING'),
            bigquery.SchemaField('page_level_4', 'STRING'),
            bigquery.SchemaField('page_level_5', 'STRING'),
            bigquery.SchemaField('device_category', 'STRING'),
            bigquery.SchemaField('operating_system', 'STRING'),
            bigquery.SchemaField('browser', 'STRING'),
            bigquery.SchemaField('browser_version', 'STRING'),
            bigquery.SchemaField('country', 'STRING'),
            bigquery.SchemaField('source', 'STRING'),
            bigquery.SchemaField('medium', 'STRING'),
            bigquery.SchemaField('campaign', 'STRING'),
            bigquery.SchemaField('ad_content', 'STRING'),
            bigquery.SchemaField('pageviews', 'INTEGER'),
            bigquery.SchemaField('unique_pageviews', 'INTEGER'),
            bigquery.SchemaField('entrances', 'INTEGER'),
            bigquery.SchemaField('exits', 'INTEGER'),
            bigquery.SchemaField('non_exit_pageviews', 'INTEGER'),
            bigquery.SchemaField('total_time_on_page', 'FLOAT'),
            bigquery.SchemaField('total_events', 'INTEGER'),
            bigquery.SchemaField('unique_events', 'INTEGER'),
            bigquery.SchemaField('single_page_sessions', 'INTEGER'),
            bigquery.SchemaField('bounces', 'INTEGER'),
            bigquery.SchemaField('page_name', 'STRING')
        ]  # Define schema
        load_job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field='date',
        )
        load_job_config.clustering_fields = ['page_name', 'country', 'locale', 'medium']
        load_job_config.write_disposition = 'WRITE_APPEND'  # Options are WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        load_job_config.destination = table_ref
        sql = f"""
        WITH parameters as (
        SELECT
          '{next_load_date}' as startDate,
          '{next_load_date}' as endDate
        ),
        
        pageMetricsTable as (
          -- This long method for the pagemetrics table has been used because of calculating time on page
          SELECT
            date,
            pagePath,
            pagePathLevel1,
            deviceCategory,
            operatingSystem,
            language,
            browser,
            browserVersion,
            country,
            source,
            medium,
            campaign,
            adContent,
            COUNT(*) AS pageviews,
            COUNT(DISTINCT CONCAT(visitIdentifier, pagePath)) as uniquePageviews,
            SUM(IF(isEntrance IS NOT NULL, 1, 0)) as entrances,
            SUM(IF(isExit IS NOT NULL,1,0)) AS exits,
            -- Single Page sessions used to calculate bounce rate
            SUM(singlePageSessions) as singlePageSessions,
            SUM(bounces) as bounces,
            SUM(time_on_page) AS totalTimeOnPage
          FROM (
            SELECT
              date,
              visitIdentifier,
              fullVisitorId,
              visitStartTime,
              pagePath,
              pagePathLevel1,
              deviceCategory,
              operatingSystem,
              language,
              browser,
              browserVersion,
              country,
              source,
              medium,
              campaign,
              adContent,
              hit_time,
              hitNumber,
              type,
              isExit,
              isEntrance,
              CASE WHEN hitNumber = firstInteraction THEN visits ELSE 0 END as singlePageSessions,
              CASE WHEN hitNumber = firstInteraction THEN bounces ELSE 0 END as bounces,
              CASE WHEN isExit IS NOT NULL THEN lastInteraction - hit_time ELSE next_pageview - hit_time END AS time_on_page
            FROM (
              SELECT
                date,
                visitIdentifier,
                fullVisitorId,
                visitStartTime,
                pagePath,
                pagePathLevel1,
                hit_time,
                type,
                isExit,
                isEntrance,
                hitNumber,
                deviceCategory,
                operatingSystem,
                language,
                browser,
                browserVersion,
                country,
                source,
                medium,
                campaign,
                adContent,
                firstInteraction,
                lastInteraction,
                LEAD(hit_time) OVER (PARTITION BY fullVisitorId, visitStartTime ORDER BY hit_time) AS next_pageview,
                visits,
                bounces
              FROM (
                SELECT
                  date,
                  visitIdentifier,
                  fullVisitorId,
                  visitStartTime,
                  pagePath,
                  pagePathLevel1,
                  type,
                  isExit,
                  isEntrance,
                  hitNumber,
                  deviceCategory,
                  operatingSystem,
                  language,
                  browser,
                  browserVersion,
                  country,
                  source,
                  medium,
                  campaign,
                  adContent,
                  hit_time,
                  firstInteraction,
                  lastInteraction,
                  visits,
                  bounces
                FROM (
                  SELECT
                    date,
                    CONCAT(CAST(fullVisitorId AS string),CAST(visitId AS string)) as visitIdentifier,
                    fullVisitorId,
                    visitStartTime,
                    hits.page.pagePath,
                    hits.page.pagePathLevel1,
                    hits.type,
                    hits.isExit,
                    hits.isEntrance,
                    hits.hitNumber,
                    device.deviceCategory,
                    device.operatingSystem,
                    device.language,
                    device.browser,
                    SPLIT(device.browserVersion, '.')[offset(0)] as browserVersion,
                    geoNetwork.country,
                    trafficSource.source,
                    trafficSource.medium,
                    trafficSource.campaign,
                    trafficSource.adContent,
                    hits.time / 1000 AS hit_time,
                    MIN(IF(hits.isInteraction IS NOT NULL, hits.hitNumber, 0)) OVER (PARTITION BY fullVisitorId, visitStartTime) AS firstInteraction,
                    MAX(IF(hits.isInteraction IS NOT NULL, hits.time / 1000, 0)) OVER (PARTITION BY fullVisitorId, visitStartTime) AS lastInteraction,
                    totals.visits,
                    totals.bounces
                  FROM
                    `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
                    UNNEST(hits) AS hits
                  WHERE
                    _TABLE_SUFFIX >= (SELECT startDate FROM parameters)
                    AND _TABLE_SUFFIX <= (SELECT endDate FROM parameters))
                WHERE
                  type = 'PAGE')
                  )
            )
          GROUP BY
             date, pagePath, pagePathLevel1, deviceCategory, operatingSystem, language, browser, browserVersion, country, source, medium, campaign, adContent),
        
        bouncesTable as (
            -- Select bounces specific metrics by dimensions
        SELECT
          date,
          pagePath,
          pagePathLevel1,
          deviceCategory,
          operatingSystem,
          language,
          browser,
          browserVersion,
          country,
          source,
          medium,
          campaign,
          adContent,
          SUM(CASE WHEN hitNumber = first_interaction THEN visits ELSE 0 END) as singlePageSessions,
          SUM(CASE WHEN hitNumber = first_interaction THEN bounces ELSE 0 END) as bounces,
          SUM(exits) as exits
        FROM (
        SELECT
          date,
          CONCAT(CAST(fullVisitorId AS string),CAST(visitId AS string)) as visitIdentifier,
          visitStartTime,
          hits.page.pagePath,
          hits.page.pagePathLevel1,
          hits.type,
          hits.isExit,
          hits.isEntrance,
          hits.hitNumber,
          device.deviceCategory,
          device.operatingSystem,
          device.language,
          device.browser,
          SPLIT(device.browserVersion, '.')[offset(0)] as browserVersion,
          geoNetwork.country,
          trafficSource.source,
          trafficSource.medium,
          trafficSource.campaign,
          trafficSource.adContent,
          totals.visits,
          totals.bounces,
          MIN(IF(hits.isInteraction IS NOT NULL, hits.hitNumber, 0)) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_interaction,
          CASE WHEN hits.isExit IS NOT NULL THEN 1 ELSE 0 END as exits
        FROM
          `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
          UNNEST (hits) as hits
        WHERE
          _TABLE_SUFFIX >= (SELECT startDate FROM parameters)
          AND _TABLE_SUFFIX <= (SELECT endDate FROM parameters))
        GROUP BY
          date, pagePath, pagePathLevel1, deviceCategory, operatingSystem, language, browser, browserVersion, country, source, medium, campaign, adContent),
        
        
        eventsTable as(
        SELECT
          date,
          pagePath,
          pagePathLevel1,
          deviceCategory,
          operatingSystem,
          language,
          browser,
          browserVersion,
          country,
          source,
          medium,
          campaign,
          adContent,
          SUM(totalEvents) as totalEvents,
          COUNT(DISTINCT CONCAT(visitIdentifier, eventID)) as uniqueEvents
        FROM (
            SELECT
              date AS date,
              CONCAT(CAST(fullVisitorId AS string),CAST(visitId AS string)) as visitIdentifier,
              hits.page.pagePath as pagePath,
              hits.page.pagePathLevel1,
              device.deviceCategory,
              device.operatingSystem,
              device.language,
              device.browser as browser,
              SPLIT(device.browserVersion, '.')[offset(0)] as browserVersion,
              geoNetwork.country,
              trafficSource.source,
              trafficSource.medium,
              trafficSource.campaign,
              trafficSource.adContent,
              count(*) as totalEvents,
              CONCAT(hits.eventInfo.eventCategory, COALESCE(hits.eventInfo.eventaction, ''), COALESCE(hits.eventInfo.eventLabel, '')) as eventID
            FROM
              `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
              UNNEST (hits) AS hits
            WHERE
              _TABLE_SUFFIX >= (SELECT startDate FROM parameters)
              AND _TABLE_SUFFIX <= (SELECT endDate FROM parameters)
              AND hits.type = 'EVENT'
              AND hits.eventInfo.eventCategory IS NOT NULL
            GROUP BY
              date,visitIdentifier, pagePath, pagePathLevel1, deviceCategory, operatingSystem, language, browser, browserVersion, country, source, medium, campaign, adContent, eventID)
        GROUP BY date, pagePath, pagePathLevel1, deviceCategory, operatingSystem, language, browser, browserVersion, country, source, medium, campaign, adContent),
        
        joinedTable as(
        SELECT
            date,
            pagePath,
            pagePathLevel1,
            deviceCategory,
            operatingSystem,
            language,
            browser,
            browserVersion,
            country,
            source,
            medium,
            campaign,
            pageMetricsTable.adContent,
            pageviews,
            uniquePageviews,
            entrances,
            exits,
            totalTimeOnPage,
            -- non_exit_pageviews -> Denominator for avg time on page calculation
            (pageMetricsTable.pageviews - pageMetricsTable.exits) as nonExitPageviews,
            -- Single Page sessions used to calculate bounce rate
            NULL as singlePageSessions,
            NULL as bounces,
            NULL as totalEvents,
            NULL as uniqueEvents
        FROM
          pageMetricsTable
        
        UNION ALL
        SELECT
            date,
            pagePath,
            pagePathLevel1,
            deviceCategory,
            operatingSystem,
            language,
            browser,
            browserVersion,
            country,
            source,
            medium,
            campaign,
            adContent,
            NULL as pageviews,
            NULL as uniquePageviews,
            NULL as entrances,
            NULL as exits,
            NULL as totalTimeOnPage,
            -- non_exit_pageviews -> Denominator for avg time on page calculation
            NULL as nonExitPageviews,
            -- Single Page sessions used to calculate bounce rate
            singlePageSessions,
            bounces,
            NULL as totalEvents,
            NULL as uniqueEvents
        FROM
          bouncesTable
        
        UNION ALL
        SELECT
            date,
            pagePath,
            pagePathLevel1,
            deviceCategory,
            operatingSystem,
            language,
            browser,
            browserVersion,
            country,
            source,
            medium,
            campaign,
            adContent,
            NULL as pageviews,
            NULL as uniquePageviews,
            NULL as entrances,
            NULL as exits,
            NULL as totalTimeOnPage,
            -- non_exit_pageviews -> Denominator for avg time on page calculation
            NULL as nonExitPageviews,
            -- Single Page sessions used to calculate bounce rate
            NULL as singlePageSessions,
            NULL as bounces,
            totalEvents,
            uniqueEvents
        FROM
          eventsTable
        ),
        
        summaryTable as (
        SELECT
          PARSE_DATE("%Y%m%d", date) as date,
          pagePath as page,
          pagePathLevel1 as locale,
          -- splitting the pagePath to make it easier to filter on pages in dashboards
          SPLIT(split(pagePath, '?')[offset(0)], '/')[SAFE_offset(2)] as page_level_1,
          SPLIT(split(pagePath, '?')[offset(0)], '/')[SAFE_offset(3)] as page_level_2,
          SPLIT(split(pagePath, '?')[offset(0)], '/')[SAFE_offset(4)] as page_level_3,
          SPLIT(split(pagePath, '?')[offset(0)], '/')[SAFE_offset(5)] as page_level_4,
          SPLIT(split(pagePath, '?')[offset(0)], '/')[SAFE_offset(6)] as page_level_5,
          deviceCategory,
          operatingSystem,
          language,
          browser,
          browserVersion,
          country,
          source,
          medium,
          campaign,
          adContent,
          SUM(pageviews) as pageviews,
          SUM(uniquePageviews) as uniquePageviews,
          SUM(entrances) as entrances,
          -- Divide exits by pageviews to get exit rate
          SUM(exits) as exits,
          -- Non exit pageviews used to calculate avg time on page -> Total time on page / non exit pageviews
          SUM(nonExitPageviews) as nonExitPageviews,
          SUM(totalTimeOnPage) as totalTimeOnPage,
          SUM(totalEvents) as totalEvents,
          SUM(uniqueEvents) as uniqueEvents,
          -- Single Page sessions used to calculate bounce rate
          SUM(singlePageSessions) as singlePageSessions,
          SUM(bounces) as bounces
        FROM
          joinedTable
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)
        
        SELECT
          *,
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
          END as page_name
        FROM
          summaryTable
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


def run_site_metrics_update():
    # Find the last date when data was loaded into the table
    read_dataset_id = 'desktop'
    read_table_name = 'website_page_metrics'
    last_load_date = calc_last_load_date(read_dataset_id, read_table_name)

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
    read_table_name = 'website_page_metrics'
    current_date = datetime.now()
    current_date = datetime.date(current_date)

    next_load_date = current_date
    end_load_date = datetime.strptime("20200406", '%Y%m%d')
    end_load_date = datetime.date(end_load_date)

    load_new_data(read_dataset_id, read_table_name, next_load_date, end_load_date)



if __name__ == '__main__':
    run_site_metrics_update()



