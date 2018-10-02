  --Pulls together data from Fetch, GA, Corpmetrics and Telemetry to tell the story of Spend, downloads, installs, LTV
WITH
--Data pull from telemetry.corpmetrics as the root for our data system. Data from other systems will be joined to this.
  telCorp AS (
  SELECT
    submission_date_s3,
    REPLACE(REPLACE(REPLACE(REPLACE(contentCleaned,'%2528','('),'%2529',')'),'%2B',' '),'%257C','|') t_content,
    SUM(installs) AS sum_installs,
    sum(dau) as sum_dau
  FROM
    `telemetry.corpMetrics`
  GROUP BY
    1,
    2
    ),
  
--Joins with LTV data set based on source, medium, campaign, and content
  l AS (
  SELECT
  f.country,
  f.targeting,
  COUNT(DISTINCT(client_ID)) n,
  AVG(total_clv) avg_tLTV
FROM
  `ltv.ltv_v1`
LEFT JOIN
  `fetch.fetch_deduped` as f
ON
  content = f.adname
WHERE
   historical_searches < (
  SELECT
    STDDEV(historical_searches)
  FROM
    `ltv.ltv_v1`) *5 + (
  SELECT
    AVG(historical_searches)
  FROM
    `ltv.ltv_v1`) 
  and vendor IN ('Adwords',
    'Bing')
  AND f.country IN ('United States',
    'Canada',
    'Germany')
  AND targeting = 'Nonbrand Search'
  AND vendornetspend >0
GROUP BY
  1,
  2
Order by 1,2,3 desc),
  --Pulls VendorNetSpend by ad and day from FetchMme
  f AS (
  SELECT
  --turns date into a string and removes '-'
*
  FROM
    `fetch.fetch_deduped`)
  --Pulls whole table
Select country,
targeting,
sum_vendornetspend,
sum_fetchdownloads,
proj_installs,
CPD,
proj_cpi,
n,
avg_tLTV,
avg_tLTV * proj_installs as revenue,
(avg_tLTV * proj_installs) - sum_vendornetspend as profit,
(avg_tLTV * proj_installs)/sum_vendornetspend AS mcac_ltv
from 
(SELECT
  Country, 
  targeting,
  sum(vendorNetSpend) sum_vendornetspend,
--  sum(downloads) sum_downloads,
  sum(downloadsGA) sum_fetchdownloads,
  sum(downloadsGA)*.66 proj_installs,
 sum(vendornetspend)/sum(downloadsGA) as CPD,
    sum(vendornetspend)/(sum(downloadsGA)*.66) as proj_CPI
FROM
  --Fetch is the base table
  f
left JOIN
  telcorp
ON
 REPLACE(CAST(date AS STRING),'-','') = telcorp.submission_date_s3
  AND f.adname = telcorp.t_content

WHERE
vendor in ('Adwords','Bing')
AND country in ('United States','Canada','Germany')
and REPLACE(CAST(f.date AS STRING),'-','') between '20180801' and '20180830' 
AND targeting = 'Nonbrand Search'
And vendornetspend >0
Group by 1,2
ORDER BY
  1,2,3 DESC) as Qa
  Left join (SELECT
  f.country as b_country,
  f.targeting as b_targeting,
  COUNT(DISTINCT(client_ID)) n,
  AVG(total_clv) avg_tLTV
FROM
  `ltv.ltv_v1`
LEFT JOIN
  `fetch.fetch_deduped` as f
ON
  content = f.adname
WHERE
   historical_searches < (
  SELECT
    STDDEV(historical_searches)
  FROM
    `ltv.ltv_v1`) *5 + (
  SELECT
    AVG(historical_searches)
  FROM
    `ltv.ltv_v1`) 
  and vendor IN ('Adwords',
    'Bing')
  AND f.country IN ('United States',
    'Canada',
    'Germany')
  AND targeting = 'Nonbrand Search'
  AND vendornetspend >0
GROUP BY
  1,
  2
) as Qb on Qa.country = Qb.b_country AND QA.targeting =Qb.b_targeting
Order by 1,2 desc