-- Used to populate Paid Search Non-brand Country Campaign Monthly

with
  fetch_trafficking as (
    select
      adname,
      vendor,
      country,
      regexp_extract(socialstring, r"^(.+)_.+$") as campaign,
      regexp_extract(socialstring, r"^.+_(.+)$") as adgroup
    from
      `fetch.latest_trafficking`
    where
      targeting = 'Nonbrand Search'
      and vendor in ('Adwords', 'Bing')
  ),

  fetch_metrics as (
    select
      date,
      adname,
      sum(vendornetspend) as sum_vendornetspend,
      sum(downloadsga) as sum_fetch_downloads
    from
      `fetch.latest_metric`
    group by
      date,
      adname
  ),

  fetch_summary AS (
    SELECT
      EXTRACT(MONTH FROM date) AS month_num,
      country,
      campaign,
      sum(sum_vendornetspend) AS sum_vendornetspend,
      sum(sum_fetch_downloads) AS sum_fetch_downloads
    FROM
      fetch_trafficking
    LEFT JOIN
      fetch_metrics
    ON
      fetch_trafficking.adname = fetch_metrics.adname
    WHERE
          sum_vendornetspend  > 0
      AND sum_fetch_downloads > 0
      AND date BETWEEN DATE(2019, 1, 1) AND DATE(2019, 12, 31)
    group by
      month_num,
      country,
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
      _TABLE_SUFFIX NOT IN ('', 'dev')
      AND _TABLE_SUFFIX NOT LIKE 'intraday%'
      AND PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) BETWEEN DATE(2019, 1, 1) AND DATE(2019, 12, 31)
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
      EXTRACT(MONTH FROM date) AS month_num,
      country,
      campaign,
      SUM(IF(downloads > 0,1,0)) as downloads,
      SUM(IF(downloads > 0 AND browser != 'Firefox',1,0)) as non_fx_downloads
    FROM
      download_events
    GROUP BY
      month_num,
      country,
      campaign
  ),

  ltv_new_clients AS (
    SELECT
      EXTRACT(MONTH FROM PARSE_DATE('%Y-%m-%d', profile_creation_date)) AS month_num,
      CASE
        -- 2019 Tier 1 countries
        WHEN country = 'US' THEN 'United States'
        WHEN country = 'DE' THEN 'Germany'
        WHEN country = 'CA' THEN 'Canada'
        WHEN country = 'FR' THEN 'France'
        WHEN country = 'GB' THEN 'United Kingdom'
        -- NOTE: 2018 non-Tier 1 countries are included to report on the tail of
        -- the spending from last year
        WHEN country = 'PL' THEN 'Poland'
        WHEN country = 'AU' THEN 'Australia'
        WHEN country = 'NL' THEN 'Netherlands'
        WHEN country = 'CH' THEN 'Switzerland'
        -- NOTE: catch-all clause for 2019 Tier 3 countries
        ELSE 'Tier 3'
      END AS country,
      regexp_replace(campaign, r"%257C", "|") as campaign,
      count(DISTINCT client_id) AS num_installs,
      SUM(total_clv) AS sum_tLTV,
      SUM(predicted_clv_12_months) AS sum_pLTV
    FROM
      `ltv.latest_sem_clients`
    WHERE
      profile_creation_date IS NOT NULL
      -- NOTE: avoid parsing `profile_creation_date` due to invalid dates
      AND profile_creation_date >= '2019-01-01'
      AND profile_creation_date <= '2019-12-31'
      AND DATE_DIFF(PARSE_DATE('%Y-%m-%d', profile_creation_date), PARSE_DATE('%Y-%m-%d', submission_date_s3), DAY) <= 7
    GROUP BY
      month_num,
      country,
      campaign
  ),

  sem_summary AS (
    SELECT
      fetch_summary.month_num,
      fetch_summary.country,
      fetch_summary.campaign,
      SUM(fetch_summary.sum_vendornetspend) AS sum_vendornetspend,
      SUM(fetch_summary.sum_fetch_downloads) AS sum_fetch_downloads,
      SUM(downloads.non_fx_downloads) AS sum_non_fx_downloads,
      SUM(ltv_new_clients.num_installs) AS sum_installs,
      SUM(ltv_new_clients.sum_pLTV) AS sum_pLTV
    FROM
      fetch_summary
    LEFT JOIN
      downloads
    ON
          fetch_summary.month_num = downloads.month_num
      AND fetch_summary.country  = downloads.country
      AND fetch_summary.campaign = downloads.campaign
    LEFT JOIN
      ltv_new_clients
    ON
          fetch_summary.month_num = ltv_new_clients.month_num
      AND fetch_summary.country  = ltv_new_clients.country
      AND fetch_summary.campaign = ltv_new_clients.campaign
    GROUP BY
      fetch_summary.month_num,
      fetch_summary.country,
      fetch_summary.campaign
  )

SELECT
  *,
  sum_vendornetspend / sum_fetch_downloads AS cpd,
  sum_vendornetspend / sum_installs AS cpi,
  sum_pLTV - sum_vendornetspend AS net_cost_of_acquisition,
  sum_pLTV / sum_vendornetspend AS ltv_mcac
FROM
  sem_summary
