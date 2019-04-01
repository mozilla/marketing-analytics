-- Populates paid search non brand country monthly report

WITH
  fetch_sem_spend AS (
  SELECT
    date,
    adname,
    vendor,
    CASE WHEN country IN ('United States',  'Germany',  'Canada',  'United Kingdom',  'France', 'Poland', 'Australia','Netherlands','Switzerland') THEN country ELSE 'Tier3' END as country,
    REGEXP_EXTRACT(socialstring, r"^(.+)_.+$") AS campaign,
    REGEXP_EXTRACT(socialstring, r"^.+_(.+)$") AS adgroup,
    SUM(VendorNetSpend) AS vendorNetSpend,
    SUM(downloadsga) AS fetchDownloads
  FROM
    `fetch.fetch_deduped`
  WHERE
    targeting = 'Nonbrand Search'
    AND vendor IN ('Adwords',
      'Bing')
    -- TODO: Need to check if this excludes any campaigns with no spend but downloads
    AND date BETWEEN DATE(2019,1,1)
    AND DATE(2019,3,27)
  GROUP BY
    date,
    adname,
    vendor,
    country,
    campaign,
    adgroup),


  fetch_summary AS(
  SELECT
    date AS fetchDate,
    country,
    adname,
    campaign,
    adgroup,
    SUM(vendorNetSpend) AS vendorNetSpend,
    SUM(fetchDownloads) AS fetchDownloads
  FROM
    fetch_sem_spend
  GROUP BY
    fetchDate,
    country,
    adname,
    campaign,
    adgroup),


  download_events AS (
  SELECT
    date,
    CASE
    -- 2019 Tier 1 countries
      WHEN geoNetwork.country IN ('United States',  'Germany',  'Canada',  'United Kingdom',  'France') THEN geoNetwork.country
    -- NOTE: 2018 non-Tier 1 countries are included to report on the tail of
    -- the spending from last year
      WHEN geoNetwork.country IN ('Poland', 'Australia','Netherlands','Switzerland') THEN geoNetwork.country
    -- NOTE: catch-all clause for 2019 Tier 3 countries
      ELSE 'Tier 3'
    END AS country,
    fullVisitorId AS visitorId,
    visitNumber AS visitNumber,
    trafficSource.source AS source,
    trafficSource.medium AS medium,
    trafficSource.campaign AS campaign,
    trafficSource.adContent AS content,
    device.browser AS browser,
    SUM(IF (hits.eventInfo.eventAction = "Firefox Download", 1, 0)) AS downloads
  FROM
    `65789850.ga_sessions_*`, UNNEST(hits) AS hits
  WHERE
    _TABLE_SUFFIX NOT IN ('','dev')
    AND _TABLE_SUFFIX NOT LIKE 'intraday%'
    AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) BETWEEN DATE(2019, 1, 1) AND DATE(2019,3,27)
    AND hits.type = 'EVENT'
    AND hits.eventInfo.eventCategory IS NOT NULL
    AND trafficSource.source IN ('google','bing')
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
    browser ),


  downloads AS (
  SELECT
    PARSE_DATE("%Y%m%d", date) AS downloadsDate,
    content,
    SUM(IF(downloads > 0, 1, 0)) AS totalDownloads,
    SUM(IF(downloads > 0 AND browser != 'Firefox',1,0)) AS non_fx_downloads
  FROM
    download_events
  GROUP BY
    downloadsDate,
    content),


   installs AS (
  SELECT
    submission AS installsDate,
    contentCleaned AS content,
    SUM(installs) AS installs
  FROM
  `ga-mozilla-org-prod-001.desktop.desktop_corp_metrics_*`
  WHERE
    funnelOrigin = 'mozFunnel'
    AND sourceCleaned IN ('google', 'bing')
    AND mediumCleaned IN ('cpc')
    AND campaignCleaned LIKE '%NB%'
    AND submission BETWEEN DATE(2019, 1, 1) AND DATE(2019,3,27)
  GROUP BY
    installsDate,
    content),


   ltv_new_clients AS (
   SELECT
    content,
    AVG(total_clv) AS avg_tLTV,
    AVG(predicted_clv_12_months) AS avg_pLTV
   FROM
    `ltv.latest_sem_clients`
   GROUP BY
    content),


  aDAU28Days AS(
  SELECT
    content,
    AVG(additionalUserMetrics.adau_days_28d) avg_adau_days_28d
  FROM (
    (SELECT
      client_id,
      content
    FROM
      `ltv.latest_sem_clients`) AS semClients
    LEFT JOIN (
    SELECT
      client_id,
      adau_days_28d
    FROM
      `ga-mozilla-org-prod-001.ltv.v1_additional_user_metrics_*`
      WHERE
      _TABLE_SUFFIX = (SELECT MAX(_table_suffix) FROM `ga-mozilla-org-prod-001.ltv.v1_additional_user_metrics_*`)) AS additionalUserMetrics
      ON semClients.client_id = additionalUserMetrics.client_id)
    GROUP BY content),


   sem_summary AS (
    SELECT
      fetch_summary.fetchDate,
      downloads.downloadsDate as downloadsDate,
      fetch_summary.adName,
      fetch_summary.country,
      SUM(fetch_summary.vendorNetSpend) AS sum_vendorNetSpend,
      SUM(fetch_summary.fetchDownloads) AS sum_fetch_downloads,
      SUM(downloads.totalDownloads) AS totalDownloads,
      SUM(downloads.non_fx_downloads) AS sum_nonFxDownloads,
      SUM(installs.installs) as installs,
      ltv_new_clients.avg_pltv,
      SUM(installs.installs) * ltv_new_clients.avg_pltv as total_pLTV,
      aDAU28Days.avg_adau_days_28d,
      ltv_new_clients.avg_tltv,
      SUM(installs.installs) * ltv_new_clients.avg_tltv as total_tLTV
    FROM
      fetch_summary
    FULL JOIN
      downloads
    ON
      fetch_summary.fetchDate = downloads.downloadsDate
      AND fetch_summary.adname = downloads.content
    FULL JOIN
      installs
    ON
          fetch_summary.fetchDate = installs.installsDate
      AND fetch_summary.adname  = installs.content
    LEFT JOIN
      ltv_new_clients
     ON
      fetch_summary.adname = ltv_new_clients.content
    LEFT JOIN
      aDAU28Days
    ON fetch_summary.adname = aDAU28Days.content
    GROUP BY
      fetchDate,
      downloadsDate,
      country,
      adname,
      avg_pltv,
      avg_adau_days_28d,
      avg_tltv
      )


  SELECT
  FORMAT_DATE("%Y%m", CASE WHEN fetchDate IS NULL THEN downloadsDate ELSE fetchDate END) as month_num,
  CASE WHEN country IS NOT NULL THEN country ELSE 'missingAdNameTracking' END as country,
  SUM(sum_vendorNetSpend) as vendorNetSpend,
  SUM(sum_fetch_downloads) as fetchDownloadsGA,
  SUM(totalDownloads) as gaTotalDownloads,
  SUM(sum_nonFxDownloads) as gaNonFxDownloads,
  SUM(installs) as installs,
  SUM(total_pLTV) as pLTV,
  SAFE_DIVIDE(SUM(sum_vendorNetSpend), SUM(sum_fetch_downloads)) as CPD_fetch_downloads,
  SAFE_DIVIDE(SUM(sum_vendorNetSpend), SUM(installs)) as CPI,
  SUM(total_pLTV) - SUM(sum_vendorNetSpend) as net_cost_of_acquisition,
  SAFE_DIVIDE(SUM(total_pLTV), SUM(sum_vendorNetSpend)) as pltv_mcac,
  AVG(avg_adau_days_28d) as avg_adau_days_28d,
  sum(total_tLTV) as tLTV,
  SAFE_DIVIDE(SUM(total_tLTV), SUM(sum_vendorNetSpend)) as tltv_mcac
  FROM sem_summary
  GROUP BY
    month_num,
    country
  ORDER BY
    month_num DESC
