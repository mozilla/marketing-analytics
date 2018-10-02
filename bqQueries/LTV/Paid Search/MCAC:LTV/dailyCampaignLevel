 --Pulls together data from Fetch, GA, Corpmetrics and Telemetry to tell the story of Spend, downloads, installs, LTV

  --Joins with LTV data set based on source, medium, campaign, and content
 with l AS (
  SELECT
    f.country,
    f.targeting,
    REGEXP_EXTRACT(socialstring, r'(.*)_') AS l_Campaign,
    COUNT(DISTINCT(client_ID)) n,
    AVG(total_clv) avg_tLTV
  FROM
    `ltv.v1_clients_20180919`
  LEFT JOIN
    `fetch.fetch_deduped` AS f
  ON
    content = f.adname
  WHERE
    historical_searches < (
    SELECT
      STDDEV(historical_searches)
    FROM
      `ltv.v1_clients_20180919`) *5 + (
    SELECT
      AVG(historical_searches)
    FROM
      `ltv.v1_clients_20180919`)
    AND vendor IN ('Adwords',
      'Bing')
    AND f.country IN ('United States',
      'Canada',
      'Germany')
    AND targeting = 'Nonbrand Search'
    AND vendornetspend >0
  GROUP BY
    1,
    2,
    3
  ORDER BY
    1,
    2,
    3 DESC),
  --Pulls VendorNetSpend by ad and day from FetchMme
  f AS (
  SELECT
    --turns date into a string and removes '-'
    *
  FROM
    `fetch.fetch_deduped`)
  --Pulls whole table
SELECT
  country,
  targeting,
  f_date,
   f_campaign,
sum_vendornetspend,
  sum_fetchdownloads,
  proj_installs,
  CPD,
  proj_cpi,
  n,
  avg_tLTV,
  avg_tLTV * proj_installs AS revenue,
  (avg_tLTV * proj_installs) - sum_vendornetspend AS profit,
  (avg_tLTV * proj_installs)/sum_vendornetspend AS mcac_ltv
FROM (
  SELECT
    REPLACE(CAST(f.date AS STRING),'-','') as f_date,
    Country,
    targeting,
    REGEXP_EXTRACT(socialstring, r'(.*)_') AS f_campaign,
    SUM(vendorNetSpend) sum_vendornetspend,
    SUM(downloadsGA) sum_fetchdownloads,
    SUM(downloadsGA)*.66 proj_installs,
    case when sum(downloadsGA) = 0 then null else SUM(vendornetspend)/SUM(downloadsGA) end AS CPD,
    case when sum(downloadsGA) = 0 then null else SUM(vendornetspend)/(SUM(downloadsGA)*.66) end AS proj_CPI
  FROM
    --Fetch is the base table
    f
 
  WHERE
    vendor IN ('Adwords',
      'Bing')
    AND country IN ('United States',
      'Canada',
      'Germany')
    AND REPLACE(CAST(f.date AS STRING),'-','') BETWEEN '20180701'
    AND '20180922'
    AND targeting = 'Nonbrand Search'
    AND vendornetspend >0
  GROUP BY
    1,
    2,3,4
  ORDER BY
    1,
    2,
    3 DESC) AS Qa
LEFT JOIN (
  SELECT
    f.country AS b_country,
    f.targeting AS b_targeting,
    REGEXP_EXTRACT(socialstring, r'(.*)_') AS qb_Campaign,

    COUNT(DISTINCT(client_ID)) n,
    AVG(total_clv) avg_tLTV
  FROM
    `ltv.v1_clients_20180919`
  LEFT JOIN
    `fetch.fetch_deduped` AS f
  ON
    content = f.adname
  WHERE
    historical_searches < (
    SELECT
      STDDEV(historical_searches)
    FROM
      `ltv.v1_clients_20180919`) *5 + (
    SELECT
      AVG(historical_searches)
    FROM
      `ltv.v1_clients_20180919`)
    AND vendor IN ('Adwords',
      'Bing')
    AND f.country IN ('United States',
      'Canada',
      'Germany')
    AND targeting = 'Nonbrand Search'
    AND vendornetspend >0
  GROUP BY
    1,
    2,3 ) AS Qb
ON
  Qa.country = Qb.b_country
  AND QA.targeting =Qb.b_targeting
  AND QA.f_campaign = Qb.Qb_campaign
ORDER BY
  1,
  2,
  3 asc