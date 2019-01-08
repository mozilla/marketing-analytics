with
  nonbranded_search_spend AS (
    SELECT
      date,
      vendor,
      adname,
      regexp_extract(socialstring, r"^(.+)_.+$") as campaign,
      regexp_extract(socialstring, r"^.+_(.+)$") as adgroup,
      country,
      targeting,
      vendornetspend,
      downloadsGA
    FROM
      `fetch.fetch_deduped`
    WHERE
      vendor IN ('Adwords', 'Bing')
      AND country IN (
        -- Tier 1
        'United States', 'Canada', 'Germany',
        -- Tier 2
        'United Kingdom', 'France', 'Poland', 'Australia', 'Netherlands', 'Switzerland',
        -- Tier 3
        'Tier 3',
        'Argentina', 'Brazil', 'Columbia', 'Mexico', 'Peru', 'Spain', 'Venezuela',
        'Egypt', 'Ethiopia', 'Kenya', 'Nigeria', 'South Africa',
        'Ukraine',
        'Saudi Arabia', 'Turkey', 'United Arab Emirates',
        'Bangladesh', 'India', 'Indonesia', 'Malaysia', 'Pakistan', 'Philippines', 'Thailand', 'Vietnam')
      AND targeting = 'Nonbrand Search'
      AND vendornetspend > 0
      AND date BETWEEN DATE(2018, 10, 1) AND DATE(2018, 12, 31)
  ),

  weekly_spend AS (
    SELECT
      EXTRACT(WEEK FROM date) AS week_num,
      country,
      targeting,
      campaign,
      SUM(vendornetspend) AS sum_vendornetspend,
      SUM(downloadsGA) AS sum_downloadsGA
    FROM
      nonbranded_search_spend
    GROUP BY
      week_num,
      country,
      targeting,
      campaign
  ),

  session_download_events as (
    SELECT
      parse_date('%Y%m%d', date) as date,
      CASE
        -- Tier 1
        WHEN geoNetwork.country in ('United States', 'Canada', 'Germany') THEN geoNetwork.country
        -- Tier 2
        WHEN geoNetwork.country in ('United Kingdom', 'France', 'Poland', 'Australia', 'Netherlands', 'Switzerland') THEN geoNetwork.country
        -- Tier 3
        WHEN geoNetwork.country in ('Argentina', 'Brazil', 'Columbia', 'Mexico', 'Peru', 'Spain', 'Venezuela',
                                    'Egypt', 'Ethiopia', 'Kenya', 'Nigeria', 'South Africa',
                                    'Ukraine',
                                    'Saudi Arabia', 'Turkey', 'United Arab Emirates',
                                    'Bangladesh', 'India', 'Indonesia', 'Malaysia', 'Pakistan', 'Philippines', 'Thailand', 'Vietnam') THEN 'Tier 3'
      END as country,
      fullVisitorId as visitorId,
      visitNumber as visitNumber,
      trafficSource.source as source,
      trafficSource.medium as medium,
      trafficSource.campaign as campaign,
      trafficSource.adContent as content,
      device.browser as browser,
      SUM(IF (hits.eventInfo.eventAction = "Firefox Download", 1, 0)) as downloads
    FROM
      `65789850.ga_sessions_*`, unnest(hits) as hits
    WHERE
      _TABLE_SUFFIX not in ('', 'dev')
      and _TABLE_SUFFIX not like 'intraday%'
      and parse_date('%Y%m%d', _TABLE_SUFFIX) between date(2018, 10, 1) and date(2018, 12, 31)
      and hits.type = 'EVENT'
      and hits.eventInfo.eventCategory is not null
      and hits.eventInfo.eventLabel like 'Firefox for Desktop%'
      and geoNetwork.country IN (
        -- Tier 1
        'United States', 'Canada', 'Germany',
        -- Tier 2
        'United Kingdom', 'France', 'Poland', 'Australia', 'Netherlands', 'Switzerland',
        -- Tier 3
        'Tier 3',
        'Argentina', 'Brazil', 'Columbia', 'Mexico', 'Peru', 'Spain', 'Venezuela',
        'Egypt', 'Ethiopia', 'Kenya', 'Nigeria', 'South Africa',
        'Ukraine',
        'Saudi Arabia', 'Turkey', 'United Arab Emirates',
        'Bangladesh', 'India', 'Indonesia', 'Malaysia', 'Pakistan', 'Philippines', 'Thailand', 'Vietnam'
      )
      and trafficSource.campaign like '%NB%'
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

  session_downloads AS (
    SELECT
      date,
      country,
      visitorId,
      visitNumber,
      campaign,
      -- non-deduped downloads:
      SUM(downloads) AS downloads,
      SUM(IF(browser != 'Firefox', downloads, 0)) AS non_fx_downloads,
      -- deduped downloads:
      SUM(IF(downloads > 0,1,0)) as deduped_downloads,
      SUM(IF(downloads > 0 AND browser != 'Firefox',1,0)) as deduped_non_fx_downloads
    FROM
      session_download_events
    GROUP BY
      date,
      country,
      visitorId,
      visitNumber,
      campaign
  ),

  weekly_downloads as (
    select
      EXTRACT(WEEK FROM date) AS week_num,
      country,
      campaign,
      SUM(downloads) AS downloads,
      SUM(non_fx_downloads) AS non_fx_downloads,
      SUM(deduped_downloads) AS deduped_downloads,
      SUM(deduped_non_fx_downloads) AS deduped_non_fx_downloads
    from
      session_downloads
    group by
      week_num,
      country,
      campaign
  ),

  ltv_new_clients as (
    select
      extract(week from parse_date('%Y-%m-%d', profile_creation_date)) as week_num,
      CASE
        WHEN country = 'US' THEN 'United States'
        WHEN country = 'DE' THEN 'Germany'
        WHEN country = 'CA' THEN 'Canada'
        WHEN country = 'GB' THEN 'United Kingdom'
        WHEN country = 'FR' THEN 'France'
        WHEN country = 'PL' THEN 'Poland'
        WHEN country = 'AU' THEN 'Australia'
        WHEN country = 'NL' THEN 'Netherlands'
        WHEN country = 'CH' THEN 'Switzerland'
        -- FIXME: this might include non-Tier 3 countries
        ELSE 'Tier 3'
      END as country,
      campaign,
      count(distinct client_id) as num_installs,
      SUM(total_clv) as sum_tLTV
    from
      `ltv.sem_clients_*`
    where
      profile_creation_date is not null
      and profile_creation_date >= '2018-01-01'
      and profile_creation_date <= '2018-12-31'
      and extract(week from parse_date('%Y-%m-%d', profile_creation_date)) + 1 = extract(week from parse_date('%Y%m%d', _TABLE_SUFFIX))
      and date_diff(parse_date('%Y-%m-%d', profile_creation_date), parse_date('%Y-%m-%d', submission_date_s3), day) <= 7
    group by
      week_num,
      country,
      campaign
  ),

  weekly_summary as (
    select
      spend.week_num,
      spend.targeting,
      spend.country,
      sum(spend.sum_vendornetspend) as sum_vendornetspend,
      sum(spend.sum_downloadsGA) as sum_fetch_downloads,
      sum(downloads.downloads) as sum_downloads,
      sum(downloads.non_fx_downloads) as sum_non_fx_downloads,
      sum(downloads.deduped_downloads) as sum_deduped_downloads,
      sum(downloads.deduped_non_fx_downloads) as sum_deduped_non_fx_downloads,
      sum(clients.num_installs) as sum_installs,
      CASE
        WHEN SUM(spend.sum_downloadsGA) = 0 THEN 0
        ELSE SUM(spend.sum_vendornetspend) / SUM(spend.sum_downloadsGA)
      END AS fetch_cpd,
      CASE
        WHEN SUM(downloads.non_fx_downloads) = 0 THEN 0
        ELSE SUM(spend.sum_vendornetspend) / SUM(downloads.non_fx_downloads)
      END AS non_fx_cpd,
      CASE
        WHEN SUM(downloads.deduped_non_fx_downloads) = 0 THEN 0
        ELSE SUM(spend.sum_vendornetspend) / SUM(downloads.deduped_non_fx_downloads)
        END AS deduped_non_fx_cpd,
      CASE
        WHEN SUM(clients.num_installs) = 0 THEN 0
        ELSE SUM(spend.sum_vendornetspend) / SUM(clients.num_installs)
      END AS cpi,
      sum(clients.sum_tLTV) as sum_tLTV,
      CASE
        WHEN SUM(clients.num_installs) = 0 THEN 0
        ELSE SUM(clients.sum_tLTV) / SUM(clients.num_installs)
      END as avg_tLTV
    from
      weekly_spend as spend
    left join
      weekly_downloads as downloads
    on
      spend.week_num = downloads.week_num
      AND spend.country = downloads.country
      AND spend.campaign = downloads.campaign
    left join
      ltv_new_clients AS clients
    on
      spend.week_num = clients.week_num
      AND spend.country = clients.country
      AND spend.campaign = clients.campaign
    group by
      week_num,
      spend.targeting,
      spend.country
  )

SELECT
  week_num,
  targeting,
  country,
  sum_vendornetspend,
  sum_fetch_downloads,
  sum_non_fx_downloads,
  sum_deduped_downloads,
  sum_deduped_non_fx_downloads,
  sum_installs,
  fetch_cpd,
  non_fx_cpd,
  deduped_non_fx_cpd,
  CASE
    WHEN sum_fetch_downloads = 0 THEN 0
    ELSE (sum_installs / sum_fetch_downloads)
  END AS fetch_conv,
  CASE
    WHEN sum_deduped_non_fx_downloads = 0 THEN 0
    ELSE (sum_installs / sum_deduped_non_fx_downloads)
  END AS deduped_non_fx_conv,
  cpi,
  avg_tLTV,
  sum_tLTV as predicted_revenue,
  sum_tLTV - sum_vendornetspend AS net_cost_of_acquisition,
  sum_tLTV / sum_vendornetspend AS ltv_mcac,
  CASE
    WHEN sum_tLTV = 0 THEN 0
    ELSE sum_vendornetspend / sum_tLTV
  END AS mcac_ltv
FROM
  weekly_summary
ORDER BY
  week_num
