# This job pulls mozilla.org site related data and aggregates into one table
# Final dataset used to power the Desktop Growth Datastudio dashboards
# Sources are primarily google analytics properties but starting Jan 29th integration will be required for retrieving data from telemetry due to data collection migration

import os
from google.cloud import bigquery
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')

# Set environment variable to authenticate using service account
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f"""{os.environ['variables_path']}moz-mktg-prod-001-app-engine-GAMozillaProdAccess.json"""

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
        bigquery.SchemaField('device_category', 'STRING'),
        bigquery.SchemaField('operating_system', 'STRING'),
        bigquery.SchemaField('browser', 'STRING'),
        bigquery.SchemaField('language', 'STRING'),
        bigquery.SchemaField('country', 'STRING'),
        bigquery.SchemaField('standardized_country_name', 'STRING'),
        bigquery.SchemaField('source', 'STRING'),
        bigquery.SchemaField('medium', 'STRING'),
        bigquery.SchemaField('campaign', 'STRING'),
        bigquery.SchemaField('content', 'STRING'),
        bigquery.SchemaField('blog', 'STRING'),
        bigquery.SchemaField('subblog', 'STRING'),
        bigquery.SchemaField('landing_page', 'STRING'),
        bigquery.SchemaField('cleaned_landing_page', 'STRING'),
        bigquery.SchemaField('sessions', 'INTEGER'),
        bigquery.SchemaField('downloads', 'INTEGER'),
        bigquery.SchemaField('socialShare', 'INTEGER'),
        bigquery.SchemaField('newsletterSubscription', 'INTEGER')
    ]  # Define schema
    load_job_config.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field='date')
    load_job_config.clustering_fields = ['cleaned_landing_page', 'browser', 'blog', 'subblog']
    load_job_config.write_disposition = 'WRITE_APPEND'  # Options are WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
    load_job_config.destination = table_ref
    sql = f"""
        WITH parameters as(
        SELECT
          '{next_load_date}' as startDate,
          '{next_load_date}' as endDate
        ),
        
        sessionsTable as (
        SELECT
          PARSE_DATE('%Y%m%d', date) as date,
          visitIdentifier,
          deviceCategory,
          operatingSystem,
          browser,
          language,
          country,
          source,
          medium,
          campaign,
          content,
          blog,
          subblog,
          SUM(sessions) as sessions,
          FROM(
            SELECT
              date,
              visitIdentifier,
              deviceCategory,
              operatingSystem,
              browser,
              language,
              country,
              source,
              medium,
              campaign,
              content,
              CASE 
                WHEN blog LIKE "press%" THEN "press"
                WHEN blog = 'firefox' THEN 'The Firefox Frontier'
                WHEN blog = 'netPolicy' THEN 'Open Policy & Advocacy'
                WHEN lower(blog) = 'internetcitizen' THEN 'Internet Citizen'
                WHEN blog = 'futurereleases' THEN 'Future Releases'
                WHEN blog = 'careers' THEN 'Careers'
                WHEN blog = 'opendesign' THEN 'Open Design'
                WHEN blog = "" THEN "Blog Home Page"
                WHEN lower(blog) IN (
                'blog','addons','security','opendesign','nnethercote','thunderbird',
                'community','l10n','theden','webrtc','berlin','webdev','services','tanvi','laguaridadefirefox','ux',
                'fxtesteng','foundation-archive','nfroyd','sumo','javascript','page') THEN lower(blog)
                ELSE 'other' END as blog,
              CASE
                WHEN blog = "firefox" AND pagePathLevel2 IN ('ru', 'pt-br', 'pl', 'it', 'id', 'fr', 'es', 'de') THEN pagePathLevel2
                WHEN blog = "firefox" THEN "Main"
                WHEN blog LIKE "press-%" AND blog IN ('press-de', 'press-fr', 'press-es', 'press-uk', 'press-pl', 'press-it', 'press-br', 'press-nl') THEN blog
                WHEN blog LIKE "press%" THEN "Main"
                WHEN blog = 'internetcitizen' AND pagePathLevel2 IN ('de', 'fr') THEN pagePathLevel2
                ELSE "Main" END as subblog,
              ROW_NUMBER () OVER (PARTITION BY visitIdentifier ORDER BY visitIdentifier, entrance) as entryPage,
              COUNT(DISTINCT visitIdentifier) as sessions
            FROM(
                SELECT
                  date AS date,
                  CONCAT(CAST(fullVisitorId AS string),CAST(visitId AS string)) as visitIdentifier,
                  device.deviceCategory,
                  device.operatingSystem,
                  device.browser,
                  device.language,
                  geoNetwork.country as country,
                  trafficSource.source as source,
                  trafficSource.medium as medium,
                  trafficSource.campaign as campaign,
                  trafficSource.adcontent as content,
                  hits.page.pagePath as landingPage,
                  CASE WHEN hits.isEntrance IS NOT NULL THEN 1 ELSE 0 END as entrance,
                  SPLIT(hits.page.pagePathLevel1, '/')[SAFE_OFFSET(1)] as blog,
                  SPLIT(hits.page.pagePathLevel2, '/')[SAFE_OFFSET(1)] as pagePathLevel2
            FROM
                  `ga-mozilla-org-prod-001.66602784.ga_sessions_*`,
                  UNNEST (hits) AS hits
                WHERE
                  _TABLE_SUFFIX >= (SELECT parameters.startDate from parameters)
                  AND _TABLE_SUFFIX <= (SELECT parameters.endDate from parameters))
            GROUP BY
              date, visitIdentifier, deviceCategory, operatingSystem, browser, language, country, source, medium, campaign, content, landingPage, blog, subblog, entrance)
        WHERE
          entryPage = 1
        GROUP BY date, visitIdentifier, deviceCategory, operatingSystem, browser, language, country, source, medium, campaign, content, blog, subblog),
        
        -- Some pages have a landing page = (not set). This makes it difficult to match the totals for the property to the landing page totals
        -- By joining the landing page totals to the summary total, we can isolate the (not set) sessions
        landingPageTable as(
        SELECT
          date,
          visitIdentifier,
          landingPage,
          cleanedLandingPage,
          SUM(sessions) as page_sessions
        FROM(
            SELECT
              PARSE_DATE("%Y%m%d", date) AS date,
              CONCAT(CAST(fullVisitorId AS string),CAST(visitId AS string)) as visitIdentifier,
              hits.page.pagePath as landingPage,
              SPLIT(hits.page.pagePath, '?')[offset(0)] as cleanedLandingPage,
              CASE WHEN hits.isEntrance IS NOT NULL THEN 1 ELSE 0 END as sessions
            FROM
              `ga-mozilla-org-prod-001.66602784.ga_sessions_*`,
              UNNEST (hits) AS hits
            WHERE
              _TABLE_SUFFIX >= (SELECT parameters.startDate from parameters)
              AND _TABLE_SUFFIX <= (SELECT parameters.endDate from parameters))
        WHERE
          sessions != 0
        GROUP BY 1,2,3,4),
        
        goalsTable as(
        SELECT
          PARSE_DATE('%Y%m%d', date) as date,
          visitIdentifier,
          SUM(IF(downloads > 0,1,0)) as downloads,
          SUM(IF(share > 0,1,0)) as socialShare,
          SUM(IF(newsletterSubscription > 0,1,0)) as newsletterSubscription
        FROM (
          SELECT
            date AS date,
            CONCAT(CAST(fullVisitorId AS string),CAST(visitId AS string)) as visitIdentifier,
            device.browser,
            SUM(IF (hits.eventInfo.eventAction = "Firefox Download",1,0)) as downloads,
            SUM(IF (hits.eventInfo.eventAction = "share",1,0)) as share,
            SUM(IF (hits.eventInfo.eventAction = "newsletter subscription",1,0)) as newsletterSubscription
          FROM
            `ga-mozilla-org-prod-001.66602784.ga_sessions_*`,
            UNNEST (hits) AS hits
          WHERE
            _TABLE_SUFFIX >= (SELECT parameters.startDate from parameters)
            AND _TABLE_SUFFIX <= (SELECT parameters.endDate from parameters)
          GROUP BY
            date,visitIdentifier, browser)
        GROUP BY date, visitIdentifier)
        
        SELECT
          sessionsTable.date,
          sessionsTable.deviceCategory as device_category,
          sessionsTable.operatingSystem as operating_system,
          sessionsTable.browser,
          sessionsTable.language,
          sessionsTable.country,
          standardizedCountryList.standardizedCountry as standardized_country_name,
          sessionsTable.source,
          sessionsTable.medium,
          sessionsTable.campaign,
          sessionsTable.content,
          sessionsTable.blog,
          sessionsTable.subblog,
          landingPageTable.landingPage as landing_page,
          landingPageTable.cleanedLandingPage as cleaned_landing_page,
          SUM(sessionsTable.sessions) as sessions,
          SUM(goalsTable.downloads) as downloads,
          SUM(goalsTable.socialShare) as socialShare,
          SUM(goalsTable.newsletterSubscription) as newsletterSubscription
        FROM
          sessionsTable
        LEFT JOIN
          goalsTable
        ON
          sessionsTable.date = goalsTable.date
          AND sessionsTable.visitIdentifier = goalsTable.visitIdentifier
        LEFT JOIN
          landingPageTable
        ON
          sessionsTable.date = landingPageTable.date
          AND sessionsTable.visitIdentifier = landingPageTable.visitIdentifier
        LEFT JOIN
          `lookupTables.standardizedCountryList` as standardizedCountryList
        ON
          sessionsTable.country = standardizedCountryList.rawCountry
        GROUP BY
          date, deviceCategory, operatingSystem, browser, language, country, standardized_country_name, source, medium, campaign, content, blog, subblog, landingPage, cleanedLandingPage
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
    return


def run_site_metrics_update():
    # Find the last date when data was loaded into the table
    read_dataset_id = '66602784'
    read_table_name = 'blogs_landing_page_summary'
    last_load_date = calc_last_load_date(read_dataset_id, read_table_name)

    # Set dates required for loading new data
    last_load_date = datetime.strptime(last_load_date, '%Y%m%d')
    end_load_date = datetime.now() - timedelta(1)  # prior day to ensure data collection is complete
    next_load_date = last_load_date + timedelta(1)

    # Load most recent data
    load_dataset_id = read_dataset_id
    load_table_name = read_table_name
    while next_load_date <= end_load_date:
        load_new_data(load_dataset_id, load_table_name, next_load_date, end_load_date)
        # Set next load date
        next_load_date = next_load_date + timedelta(days=1)
    return


def initiate_table():
    read_dataset_id = '66602784'
    read_table_name = 'blogs_landing_page_summary'
    current_date = datetime.now()
    current_date = datetime.date(current_date)

    next_load_date = current_date
    end_load_date = datetime.strptime("20200314", '%Y%m%d')
    end_load_date = datetime.date(end_load_date)

    load_new_data(read_dataset_id, read_table_name, next_load_date, end_load_date)



if __name__ == '__main__':
    run_site_metrics_update()
    #initiate_table()


