WITH
  corpMetrics as(
    SELECT
      campaignCleaned,
      SUM(installs) AS installs
    FROM
      `ga-mozilla-org-prod-001.telemetry.corpMetrics`
    WHERE
      submission_date_s3 >= '20190101'
      AND submission_date_s3 <= '20190122'
      AND sourceCleaned IN ('google',
        'bing')
      AND mediumCleaned = 'cpc'
      AND campaignCleaned LIKE '%NB%'
    GROUP BY
      campaignCleaned
    ORDER BY
      installs DESC
  ),

  sem_clients as(
    SELECT
      regexp_replace(campaign, r"%257C", "|") AS sem_clients_campaign,
      count(DISTINCT client_id) AS sem_clients_num_installs
    FROM
      `ltv.latest_sem_clients`
    WHERE
      profile_creation_date IS NOT NULL
      -- NOTE: avoid parsing `profile_creation_date` due to invalid dates
      AND profile_creation_date >= '2019-01-01'
      AND profile_creation_date <= '2019-01-22'
      AND DATE_DIFF(PARSE_DATE('%Y-%m-%d', profile_creation_date), PARSE_DATE('%Y-%m-%d', submission_date_s3), DAY) <= 7
    GROUP BY
      campaign
  ),

  download_events AS (
    SELECT
      PARSE_DATE('%Y%m%d', date) AS date,
      CASE
        -- 2019 Tier 1 countries
        WHEN geoNetwork.country IN ('United States', 'Germany', 'Canada', 'United Kingdom', 'France') THEN geoNetwork.country
        -- NOTE: 2018 non-Tier 1 countries are included to report on the tail of
        -- the spending from last year
        WHEN geoNetwork.country IN ('Poland', 'Australia', 'Netherlands', 'Switzerland') THEN geoNetwork.country
        -- NOTE: catch-all clause for 2019 Tier 3 countries
        ELSE 'Tier 3'
      END AS country,
      fullVisitorId           AS visitorId,
      visitNumber             AS visitNumber,
      trafficSource.source    AS source,
      trafficSource.medium    AS medium,
      trafficSource.campaign  AS campaign,
      trafficSource.adContent AS content,
      device.browser          AS browser,
      SUM(IF (hits.eventInfo.eventAction = "Firefox Download", 1, 0)) AS downloads
    FROM
      `65789850.ga_sessions_*`, UNNEST(hits) AS hits
    WHERE
      _TABLE_SUFFIX NOT IN ('', 'dev')
      AND _TABLE_SUFFIX NOT LIKE 'intraday%'
      AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) BETWEEN DATE(2019, 1, 1) AND DATE(2019, 1, 22)
      AND hits.type = 'EVENT'
      AND hits.eventInfo.eventCategory IS NOT NULL
      AND trafficSource.source in ('google', 'bing')
      AND trafficSource.medium = 'cpc'
      AND trafficSource.campaign LIKE '%NB%'
    GROUP BY
      date,
      country,
      visitorId,
      visitNumber,
      source,
      medium,
      campaign,
      content,
      browser
  ),

  downloads AS (
    SELECT
      -- EXTRACT(WEEK FROM date) AS week_num,
      -- country,
      campaign,
      SUM(IF(downloads > 0,1,0)) AS downloads,
      SUM(IF(downloads > 0 AND browser != 'Firefox',1,0)) AS non_fx_downloads
    FROM
      download_events
    GROUP BY
      -- week_num,
      -- country,
      campaign
  )

SELECT
  corpMetrics.campaignCleaned,
  sem_clients.sem_clients_campaign,
  downloads.campaign,
  SUM(downloads.non_fx_downloads)                                                         AS sum_non_fx_downloads,
  SUM(corpMetrics.installs)                                                               AS corpMetrics_installs,
  SAFE_DIVIDE(SUM(corpMetrics.installs), SUM(downloads.non_fx_downloads))                 AS corpMetrics_conversion,
  SUM(sem_clients.sem_clients_num_installs)                                               AS sem_clients_num_installs,
  SAFE_DIVIDE(SUM(sem_clients.sem_clients_num_installs), SUM(downloads.non_fx_downloads)) AS sem_clients_conversion,
  (SUM(corpMetrics.installs) -  SUM(sem_clients.sem_clients_num_installs))                AS corpMetrics_sem_variance
FROM
  corpMetrics
LEFT JOIN
  sem_clients
ON
  corpMetrics.campaignCleaned = sem_clients.sem_clients_campaign
LEFT JOIN
  downloads
ON
  corpMetrics.campaignCleaned = downloads.campaign
GROUP BY
  campaignCleaned,
  sem_clients_campaign,
  downloads.campaign
ORDER BY
  corpMetrics_sem_variance DESC
