  --Pulls together data from Fetch Corpmetrics see if attribution is working like it should
WITH
  --Data pull from telemetry.corpmetrics as the root for our data system. Data from other systems will be joined to this.
  telCorp AS (
  SELECT
    submission_date_s3,
    REPLACE(REPLACE(REPLACE(REPLACE(sourceCleaned,'%2528','('),'%2529',')'),'%2B',' '),'%257C','|') t_source,
    REPLACE(REPLACE(REPLACE(REPLACE(mediumCleaned,'%2528','('),'%2529',')'),'%2B',' '),'%257C','|')t_medium,
    REPLACE(REPLACE(REPLACE(REPLACE(campaignCleaned,'%2528','('),'%2529',')'),'%2B',' '),'%257C','|') t_campaign,
    REPLACE(REPLACE(REPLACE(REPLACE(contentCleaned,'%2528','('),'%2529',')'),'%2B',' '),'%257C','|') t_content,
    SUM(installs) AS sum_installs,
    SUM(dau) AS sum_dau
  FROM
    `telemetry.corpMetrics`
  GROUP BY
    1,
    2,
    3,
    4,
    5),
  --Pulls VendorNetSpend by ad and day from FetchMme
  f AS (
  SELECT
    --turns date into a string and removes '-'
    REPLACE(CAST(date AS STRING),'-','') AS f_date,
    adName AS f_adName,
    country,
    targeting,
    vendor,
    socialstring,
    vendorNetSpend AS f_vendorNetSpend
  FROM
    `fetch.fetch_deduped`),
  dls AS (
  SELECT
    date AS date,
    source,
    medium,
    campaign,
    content,
    SUM(IF(downloads > 0,
        1,
        0)) AS downloads,
    SUM(IF(downloads > 0
        AND browser != 'Firefox',
        1,
        0)) AS nonFXDownloads,
    SUM(IF(downloads >0
        AND operatingSystem = 'Windows',
        1,
        0)) AS windows_downloads
  FROM (
    SELECT
      date AS date,
      fullVisitorId AS visitorId,
      visitNumber AS visitNumber,
      CASE
        WHEN hits.isEntrance IS TRUE THEN page.pagePath
      END AS landingPage,
      device.browser AS browser,
      device.browserVersion AS browserVersion,
      device.operatingSystem AS operatingSystem,
      geoNetwork.country AS country,
      trafficSource.source AS source,
      trafficSource.medium AS medium,
      trafficSource.campaign AS campaign,
      trafficSource.adContent AS content,
      SUM(IF (hits.eventInfo.eventAction = "Firefox Download",
          1,
          0)) AS downloads
    FROM
      `65789850.ga_sessions_*`,
      UNNEST (hits) AS hits
    WHERE
      _TABLE_SUFFIX >= '20180701'
      AND _TABLE_SUFFIX <= '20180731'
      AND hits.type = 'EVENT'
      AND hits.eventInfo.eventCategory IS NOT NULL
      AND hits.eventInfo.eventLabel LIKE "Firefox for Desktop%"
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12)
  GROUP BY
    1,
    2,
    3,
    4,
    5
  ORDER BY
    1,
    6 DESC),
    l as (
    select * from `ltv.ltv_v1`
    )
/*
SELECT
  CASE
    WHEN sum_all_downloads IS NULL THEN 'GA Null'
    ELSE 'GA Avail'
  END ga_status,
  CASE
    WHEN ssum_installs = 0 THEN 'Telem 0'
    ELSE 'Telem Avail'
  END telem_status,
  COUNT(DISTINCT(f_adname)) campaigns,
  SUM(sum_vendornetspend) sSum_vendornetspend,
  sum(ssum_installs) installs
FROM
  --Pulls whole table
  (*/
  SELECT
    f.f_Adname,
    f.socialstring fetch_socialstring,
    t_campaign telem_campaign,
    campaign ga_campaign,
    SUM(f_vendorNetspend) sum_vendornetspend,
    SUM(downloads) sum_all_downloads,
    SUM(windows_downloads) sum_windows_downloads,
    SUM(sum_installs) sSum_installs,
    SUM(sum_installs) / (SUM(windows_downloads)+1) install_2_download_rate,
    SUM(sum_dau) sSum_dau,
    SUM(IF(t_campaign IS NULL,
        1,
        0)) Null_GA_campaign_days,
    SUM(IF(t_content IS NULL,
        1,
        0)) sum_null_corp_campaign_days,
    --  sum(if((windows_downloads-sum_installs)/windows_downloads > .4,1,0)) sum_dl_ins_below40,
    --  sum(if((windows_downloads-sum_installs)/windows_downloads > .1,1,0)) dl_ins_below10,
    --  sum(if((windows_downloads-sum_installs)/windows_downloads = 1,1,0)) dl_ins_zero,
    COUNT(f_adname) count_adname
  FROM
    --Fetch is the base table
    f
  LEFT JOIN
    telcorp
  ON
    f.f_date = telcorp.submission_date_s3
    AND f.f_adname = telcorp.t_content
  LEFT JOIN
    dls
  ON
    f.f_date = dls.date
    AND f.f_adname = dls.content
  WHERE
    submission_date_s3 BETWEEN '20180701'
    AND '20180731'
   AND (socialstring LIKE 'Firefox|BN|Search|NB|Exact|SP|US|EN|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|BMM|SP|DE|TS|DK|Text|Browser%'
      OR socialstring LIKE 'Firefox|BN|Search|NB|Exact|SP|DE|TS|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|Exact|SP|US|EN|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|Broad|SP|US|EN|DK|Text|Browser%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|BMM|SP|DE|TS|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|Exact|SP|DE|TS|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|BN|Search|NB|Exact|SP|CA|EN|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|BMM|SP|US|EN|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|BN|Search|NB|Exact|SP|DE|EN|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|Exact|SP|US|EN|DK|Text|Browser%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|Exact|SP|DE|TS|DK|Text|Browser%'
      OR socialstring LIKE 'Firefox|BN|Search|NB|BMM|SP|DE|TS|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|Exact|SP|CA|EN|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|BN|Search|NB|BMM|SP|DE|EN|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|Broad|SP|CA|EN|DK|Text|Browser%'
      OR socialstring LIKE 'Firefox|BN|Search|NB|BMM|SP|CA|EN|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|Broad|SP|DE|EN|DK|Text|Browser%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|BMM|SP|CA|EN|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|Broad|SP|US|EN|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|BN|Search|NB|BMM|SP|CA|EN|DK|Text|Browser%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|BMM|SP|DE|EN|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|Exact|SP|DE|EN|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|Broad|SP|CA|EN|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|Broad|SP|DE|EN|DK|Text|Competitor%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|Exact|SP|CA|EN|DK|Text|Browser%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|Exact|SP|DE|EN|DK|Text|Browser%'
      OR socialstring LIKE 'Firefox|BN|Search|NB|BMM|SP|DE|EN|DK|Text|Browser%'
      OR socialstring LIKE 'Brand-PL-EN-GGL-BMM%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|Exact|SP|US|EN|DK|Text|Web Trackers%'
      OR socialstring LIKE 'Firefox|GG|Search|NB|BMM|SP|US|EN|DK|Text|Web Trackers%') 
  GROUP BY
    1,
    2,3,4
   ORDER BY
  SUM_VENDORNETSPEND DESC /**/
   /* )
GROUP BY
  1,
  2
ORDER BY
  Ga_status, sSUM_VENDORNETSPEND DESC */