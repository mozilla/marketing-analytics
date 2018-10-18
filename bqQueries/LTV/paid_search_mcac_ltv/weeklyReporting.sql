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
    REPLACE(REPLACE(REPLACE(REPLACE(content,'%2528','('),'%2529',')'),'%2B',' '),'%257C','|') l_content,
    COUNT(DISTINCT(client_ID)) n,
    AVG(total_clv) avg_tLTV
  FROM
    `ltv.ltv_v1`
  WHERE
    customer_age < 155
    AND historical_searches < (
    SELECT
      STDDEV(historical_searches)
    FROM
      `ltv.ltv_v1`) *5 + (
    SELECT
      AVG(historical_searches)
    FROM
      `ltv.ltv_v1`)
  GROUP BY
    1),
  --Pulls VendorNetSpend by ad and day from FetchMme
  f AS (
  SELECT
  --turns date into a string and removes '-'
*
  FROM
    `fetch.fetch_deduped`)
  --Pulls whole table
SELECT
  Country, 
  targeting,
  case when REGEXP_EXTRACT(socialstring, r'(.*)_') like '%Competitor%' then 'Competitor' 
    when REGEXP_EXTRACT(socialstring, r'(.*)_') like '%Browser%' then 'Browser' end as AGroup, 
  sum(vendorNetSpend) sum_vendornetspend,
  sum(downloadsGA) sum_downloads,
    sum(vendornetspend)/sum(downloadsGA) as CPD,
    sum(downloadsGA)
    sum(vendornetspend)/(sum(downloadsGA)*.61) as proj_CPI,
    sum(sum_installs) summ_installs
--  sum(sum_dau) summ_dau
--  sum(n) n_ltv,
--  avg(avg_tLTV),
--  Sum(sum_installs * avg_tLTV) AS proj_rev
FROM
  --Fetch is the base table
  f
left JOIN
  telcorp
ON
 REPLACE(CAST(date AS STRING),'-','') = telcorp.submission_date_s3
  AND f.adname = telcorp.t_content
  
/*LEFT JOIN
  telcorp
ON
  f.adname = l.l_content
  AND f.f_date = submission_date_s3*/
WHERE
vendor in ('Bing')
AND country in ('United States','Canada','Germany')
and REPLACE(CAST(f.date AS STRING),'-','') between '20180701' and '20180731' 
AND targeting = 'Nonbrand Search'
And vendornetspend >0
Group by 1,2,3
ORDER BY
  1,2,3 DESC