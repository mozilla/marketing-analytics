# This job pulls desktop related metrics data from telemetry, cleans and prepares it for joining with google analytics acquisition data
# Final dataset used to power the Final Dataset used to power the Datastudio Firefox Desktop Growth Dashboards
# Sources are telemetry and mozilla.org google analytics data stored in bigquery

import os
from google.cloud import bigquery
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')

# Set environment variable to authenticate using service account
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
    max(_table_suffix) AS last_load_date
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


def load_new_snipppet_data(dataset_id, table_name, next_load_date, end_load_date):
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

        # Set Sample Size Multiplier
        sample_rate_change_date = datetime.strptime('20171031', '%Y%m%d')  # date sampling changed from 1% to 0.1%
        if datetime.strptime(next_load_date, '%Y%m%d') < sample_rate_change_date:
            sample_multiplier = 100
        else:
            sample_multiplier = 1000

        # Configure load job
        dataset_ref = client.dataset(load_dataset_id)
        table_ref = dataset_ref.table(load_table_id)
        load_job_config = bigquery.QueryJobConfig()  # load job call
        load_job_config.schema = [
            bigquery.SchemaField('date', 'DATE'),
            bigquery.SchemaField('snippetID', 'STRING'),
            bigquery.SchemaField('country', 'STRING'),
            bigquery.SchemaField('site', 'STRING'),
            bigquery.SchemaField('impression', 'INTEGER'),
            bigquery.SchemaField('snippetBlocked', 'INTEGER'),
            bigquery.SchemaField('clicks', 'INTEGER'),
            bigquery.SchemaField('otherSnippetInteractions', 'INTEGER'),
            bigquery.SchemaField('sessions', 'INTEGER'),
            bigquery.SchemaField('addonInstallsTotal', 'INTEGER'),
            bigquery.SchemaField('addonInstallsGoalComp', 'INTEGER'),
            bigquery.SchemaField('themeInstallsTotal', 'INTEGER'),
            bigquery.SchemaField('themeInstallsGoalComp', 'INTEGER'),
            bigquery.SchemaField('donations', 'INTEGER')
        ]  # Define schema
        load_job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field='date',
        )
        load_job_config.write_disposition = 'WRITE_APPEND'  # Options are WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        load_job_config.destination = table_ref
        sql = f"""
                WITH impressionData AS(
            SELECT
                visitData.date,
                visitData.snippetID,
                visitData.country,
                visitData.eventCategory,
                -- Get statistics for top 3 events. All other = other
                CASE WHEN eventCategory = 'impression' THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS impression,
                CASE WHEN eventCategory = 'snippet-blocked' THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS snippetBlocked,
                CASE WHEN eventCategory = 'click' OR eventCategory = 'button-click' THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS clicks,
                CASE WHEN eventCategory NOT IN('impression','snippet-blocked', 'click','button-click') THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS other
            FROM (
                SELECT
                date,
                geoNetwork.country,
                fullVisitorId,
                eventInfo.eventAction AS snippetID,
                eventInfo.eventCategory
                FROM
                `ga-mozilla-org-prod-001.125230768.ga_sessions_*`,
                UNNEST (hits) AS hits
                WHERE
                _TABLE_SUFFIX = '{load_table_suffix}'
                GROUP BY 1,2,3,4,5) AS visitData
            GROUP BY
                1,2,3,4
            ORDER BY 4 DESC),

            -- Pull data from addons.mozilla.org

            addonsData AS(SELECT
                date AS date,
                trafficSource.keyword AS snippetID,
                geoNetwork.country AS country,
                SUM(totals.visits) AS sessions,
                SUM((SELECT SUM(DISTINCT IF (REGEXP_CONTAINS(hits.eventInfo.eventCategory, '^AMO (?:Addon|Theme|Addon / Theme) Installs$') AND hits.eventInfo.eventAction = 'addon',1,0)) FROM UNNEST(hits) hits)) AS sessionsInstallingAddons,
                SUM((SELECT SUM(IF (REGEXP_CONTAINS(hits.eventInfo.eventCategory, '^AMO (?:Addon|Theme|Addon / Theme) Installs$') AND hits.eventInfo.eventAction = 'addon',1,0)) FROM UNNEST(hits) hits)) AS totalAddonsInstalled,
                SUM((SELECT SUM(DISTINCT IF (REGEXP_CONTAINS(hits.eventInfo.eventCategory, '^AMO (?:Addon|Theme|Addon / Theme) Installs$') AND hits.eventInfo.eventAction = 'theme',1,0)) FROM UNNEST(hits) hits)) AS sessionsInstallingThemes,
                SUM((SELECT SUM(IF (REGEXP_CONTAINS(hits.eventInfo.eventCategory, '^AMO (?:Addon|Theme|Addon / Theme) Installs$') AND hits.eventInfo.eventAction = 'theme',1,0)) FROM UNNEST(hits) hits)) AS totalThemesInstalled
            FROM `ga-mozilla-org-prod-001.67693596.ga_sessions_*`
            WHERE
            _TABLE_SUFFIX = '{load_table_suffix}'
            AND trafficSource.medium = 'snippet'
            GROUP BY 1,2,3
            ORDER BY 2 ASC, 4 DESC),

            -- Pull data from mozilla.org
            mozorgData AS(
            SELECT
            date as date,
            trafficSource.keyword as snippetID,
            geoNetwork.country as country,
            SUM(totals.visits) AS sessions
            FROM
            `ga-mozilla-org-prod-001.65789850.ga_sessions_*`
            WHERE
            _TABLE_SUFFIX = '{load_table_suffix}'
            AND trafficSource.medium = 'snippet'
            GROUP By 1,2,3
            ORDER BY 4 DESC
            ),

            -- Pull data from blog.mozilla.org
            blogData AS(
            SELECT
              date as date,
              trafficSource.keyword as snippetID,
              geoNetwork.country as country,
              SUM(totals.visits) AS sessions
            FROM
              `ga-mozilla-org-prod-001.66602784.ga_sessions_*`
            WHERE
              _TABLE_SUFFIX = '{load_table_suffix}'
              AND trafficSource.medium = 'snippet'
            GROUP By 1,2,3
            ORDER BY 4 DESC
            ),

            -- Pull data from testpilot.firefox.com
            testPilotData AS(
            SELECT
              date as date,
              trafficSource.keyword as snippetID,
              geoNetwork.country as country,
              SUM(totals.visits) AS sessions
            FROM
              `ga-mozilla-org-prod-001.106368739.ga_sessions_*`
            WHERE
              _TABLE_SUFFIX = '{load_table_suffix}'
              AND trafficSource.medium = 'snippet'
            GROUP By 1,2,3
            ORDER BY 4 DESC
            ),

            -- Pull data from developer.mozilla.org
            developerData AS(
            SELECT
              date as date,
              trafficSource.keyword as snippetID,
              geoNetwork.country as country,
              SUM(totals.visits) AS sessions
            FROM
              `ga-mozilla-org-prod-001.66726481.ga_sessions_*`
            WHERE
              _TABLE_SUFFIX = '{load_table_suffix}'
              AND trafficSource.medium = 'snippet'
            GROUP By 1,2,3
            ORDER BY 4 DESC
            ),

            -- Pull data from support.mozilla.org
            sumoData AS(
            SELECT
              date as date,
              trafficSource.keyword as snippetID,
              geoNetwork.country as country,
              SUM(totals.visits) AS sessions
            FROM
              `ga-mozilla-org-prod-001.65912487.ga_sessions_*`
            WHERE
              _TABLE_SUFFIX = '{load_table_suffix}'
              AND trafficSource.medium = 'snippet'
            GROUP By 1,2,3
            ORDER BY 4 DESC
            ),

            -- Pull data from hacks.mozilla.org
            hacksData AS(
            SELECT
              date as date,
              trafficSource.keyword as snippetID,
              geoNetwork.country as country,
              SUM(totals.visits) AS sessions
            FROM
              `ga-mozilla-org-prod-001.65887927.ga_sessions_*`
            WHERE
              _TABLE_SUFFIX = '{load_table_suffix}'
              AND trafficSource.medium = 'snippet'
            GROUP By 1,2,3
            ORDER BY 4 DESC
            ),

            -- Pull data from donate.mozilla.org
            donateData AS(
            SELECT
                date AS date,
                trafficSource.keyword AS snippetID,
                geoNetwork.country AS country,
                SUM(totals.visits) AS sessions,
                SUM((SELECT SUM(DISTINCT IF(REGEXP_CONTAINS(page.pagePath, '/thank-you/'),1,0)) FROM UNNEST(hits) )) AS donations
              FROM
                `ga-mozilla-org-prod-001.105783219.ga_sessions_*`
              WHERE
              _TABLE_SUFFIX = '{load_table_suffix}'
              AND trafficSource.medium = 'snippet'
              GROUP BY 1,2,3
              ORDER BY 2 ASC,4 DESC
            )

            -- Aggregate by date, snippetID, country and site
            SELECT
              PARSE_DATE('%Y%m%d', impressions.date) as date,
              impressions.snippetID,
              impressions.country,
              'snippets tracking' as site,
              SUM(impressions.impression)*{sample_multiplier} AS impression,
              SUM(impressions.snippetBlocked)*{sample_multiplier} AS snippetBlocked,
              SUM(impressions.clicks)*{sample_multiplier} AS clicks,
              SUM(impressions.other)*{sample_multiplier} as otherSnippetInteractions,
              NULL as sessions,
              NULL as addonInstallsTotal,
              NULL as addonInstallsGoalComp,
              NULL as themeInstallsTotal,
              NULL as themeInstallsGoalComp,
              NULL as donations
            FROM impressionData as impressions
            GROUP By 1,2,3,4

            -- Join addons data
            UNION ALL
            SELECT
              PARSE_DATE('%Y%m%d', addonsData.date) as date,
              addonsData.snippetID,
              addonsData.country,
              'addons.mozilla.org' as site,
              NULL as impression,
              NULL as snippetBlocked,
              NULL as clicks,
              NULL as otherSnippetInteractions,
              SUM(addonsData.sessions) as sessions,
              SUM(addonsData.totalAddonsInstalled) as addonInstallsTotal,
              SUM(addonsData.sessionsInstallingAddons) as addonInstallsGoalComp,
              SUM(addonsData.totalThemesInstalled) as themeInstallsTotal,
              SUM(addonsData.sessionsInstallingThemes) as themeInstallsGoalComp,
              NULL as donations
            FROM addonsData
            GROUP BY 1,2,3,4

            -- Join mozilla.org data
            UNION ALL
            SELECT
              PARSE_DATE('%Y%m%d', mozorgData.date) as date,
              mozorgData.snippetID,
              mozorgData.country,
              'mozilla.org' as site,
              NULL as impression,
              NULL as snippetBlocked,
              NULL as clicks,
              NULL as otherSnippetInteractions,
              SUM(mozorgData.sessions) as sessions,
              NULL as addonInstallsTotal,
              NULL as addonInstallsGoalComp,
              NULL as themeInstallsTotal,
              NULL as themeInstallsGoalComp,
              NULL as donations
            FROM mozorgData
            GROUP BY 1,2,3,4

            -- Join blog.mozilla.org data
            UNION ALL
            SELECT
              PARSE_DATE('%Y%m%d', blogData.date) as date,
              blogData.snippetID,
              blogData.country,
              'blog.mozilla.org' as site,
              NULL as impression,
              NULL as snippetBlocked,
              NULL as clicks,
              NULL as otherSnippetInteractions,
              SUM(blogData.sessions) as sessions,
              NULL as addonInstallsTotal,
              NULL as addonInstallsGoalComp,
              NULL as themeInstallsTotal,
              NULL as themeInstallsGoalComp,
              NULL as donations
            FROM blogData
            GROUP BY 1,2,3,4

            -- Join testpilot.firefox.com data
            UNION ALL
            SELECT
              PARSE_DATE('%Y%m%d', testPilotData.date) as date,
              testPilotData.snippetID,
              testPilotData.country,
              'testpilot.firefox.com' as site,
              NULL as impression,
              NULL as snippetBlocked,
              NULL as clicks,
              NULL as otherSnippetInteractions,
              SUM(testPilotData.sessions) as sessions,
              NULL as addonInstallsTotal,
              NULL as addonInstallsGoalComp,
              NULL as themeInstallsTotal,
              NULL as themeInstallsGoalComp,
              NULL as donations
            FROM testPilotData
            GROUP BY 1,2,3,4

            -- Join developer.mozilla.org data
            UNION ALL
            SELECT
              PARSE_DATE('%Y%m%d', developerData.date) as date,
              developerData.snippetID,
              developerData.country,
              'developer.mozilla.org' as site,
              NULL as impression,
              NULL as snippetBlocked,
              NULL as clicks,
              NULL as otherSnippetInteractions,
              SUM(developerData.sessions) as sessions,
              NULL as addonInstallsTotal,
              NULL as addonInstallsGoalComp,
              NULL as themeInstallsTotal,
              NULL as themeInstallsGoalComp,
              NULL as donations
            FROM developerData
            GROUP BY 1,2,3,4

            -- Join support.mozilla.org data
            UNION ALL
            SELECT
              PARSE_DATE('%Y%m%d', sumoData.date) as date,
              sumoData.snippetID,
              sumoData.country,
              'support.mozilla.org' as site,
              NULL as impression,
              NULL as snippetBlocked,
              NULL as clicks,
              NULL as otherSnippetInteractions,
              SUM(sumoData.sessions) as sessions,
              NULL as addonInstallsTotal,
              NULL as addonInstallsGoalComp,
              NULL as themeInstallsTotal,
              NULL as themeInstallsGoalComp,
              NULL as donations
            FROM sumoData
            GROUP BY 1,2,3,4

            -- Join hacks.mozilla.org data
            UNION ALL
            SELECT
              PARSE_DATE('%Y%m%d', hacksData.date) as date,
              hacksData.snippetID,
              hacksData.country,
              'support.mozilla.org' as site,
              NULL as impression,
              NULL as snippetBlocked,
              NULL as clicks,
              NULL as otherSnippetInteractions,
              SUM(hacksData.sessions) as sessions,
              NULL as addonInstallsTotal,
              NULL as addonInstallsGoalComp,
              NULL as themeInstallsTotal,
              NULL as themeInstallsGoalComp,
              NULL as donations
            FROM hacksData
            GROUP BY 1,2,3,4

            -- Join donate.mozilla.org data
            UNION ALL
            SELECT
              PARSE_DATE('%Y%m%d', donateData.date) as date,
              donateData.snippetID,
              donateData.country,
              'donate.mozilla.org' as site,
              NULL as impression,
              NULL as snippetBlocked,
              NULL as clicks,
              NULL as otherSnippetInteractions,
              SUM(donateData.sessions) as sessions,
              NULL as addonInstallsTotal,
              NULL as addonInstallsGoalComp,
              NULL as themeInstallsTotal,
              NULL as themeInstallsGoalComp,
              SUM(donateData.donations) as donations
            FROM donateData
            GROUP BY 1,2,3,4

            -- Join telemetry tracking data
            UNION ALL
            SELECT 
              sendDate, 
              messageID,
              countryCode,
              'telemetry tracking' as site,
              SUM(impressions) as impression,
              SUM(blocks) as snippetBlocked,
              SUM(clicks) as clicks,
              NULL as other,
              NULL as sessions,
              NULL as addonInstallsTotal,
              NULL as addonInstallsGoalComp,
              NULL as themeInstallsTotal,
              NULL as themeInstallsGoalComp,
              NULL as donations
            FROM `ga-mozilla-org-prod-001.snippets.snippets_telemetry_tracking_*`
            WHERE
                _TABLE_SUFFIX = '{load_table_suffix}'
            GROUP BY 1,2,3,4
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


def run_snippets_performance_update():
    # Find the last date when data was loaded into the table
    read_dataset_id = 'snippets'
    read_table_name = 'snippets_performance'
    last_load_date = calc_last_load_date(read_dataset_id, read_table_name)

    # Set dates required for loading new data
    last_load_date = datetime.strptime(last_load_date, '%Y%m%d')
    end_load_date = datetime.now() - timedelta(1)  # prior day to ensure data collection is complete
    next_load_date = last_load_date + timedelta(1)

    # Load most recent data
    load_dataset_id = read_dataset_id
    load_table_name = read_table_name
    load_new_snipppet_data(load_dataset_id, load_table_name, next_load_date, end_load_date)
    return


if __name__ == '__main__':
    run_snippets_performance_update()