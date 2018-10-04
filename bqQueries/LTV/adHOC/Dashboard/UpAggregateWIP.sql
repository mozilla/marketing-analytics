--queries for segments, default browser, FxA, Multi-Desktop Sync, Mobile-Sync
--US AVG
--Select 

--From
--(
SELECT
 'US Avg.', avg(total_clv) avg_LTV, sum(total_clv)*100 sum_LTV, count(distinct(client_id)) n, count(distinct(client_id))*100 population
FROM
  `ltv.ltv_v1`
WHERE
 country = 'US' 
 AND 
-- Exclude outliers
 historical_searches < (
  SELECT
    STDDEV(historical_searches)
  FROM
    `ltv.ltv_v1`) *2.5 + (
  SELECT
    AVG(historical_searches)
  FROM
    `ltv.ltv_v1`)
Union All
SELECT
 'Default Browser', avg(total_clv) avg_LTV, sum(total_clv)*100 sum_LTV, count(distinct(client_id)) n, count(distinct(client_id))*100 population
FROM
  `ltv.ltv_v1`
WHERE
 country = 'US'
 AND 
 is_default_browser = 't'
 AND 
-- Exclude outliers
 historical_searches < (
  SELECT
    STDDEV(historical_searches)
  FROM
    `ltv.ltv_v1`) *2.5 + (
  SELECT
    AVG(historical_searches)
  FROM
    `ltv.ltv_v1`)
Union All
----sync_configured
SELECT
  'Sync Configured', avg(total_clv) avg_LTV, sum(total_clv)*100 sum_LTV, count(distinct(client_id)) n, count(distinct(client_id))*100 population
FROM
  `ltv.ltv_v1`
WHERE
 country = 'US'
 AND 
 sync_configured = 't'
 AND 
-- Exclude outliers
 historical_searches < (
  SELECT
    STDDEV(historical_searches)
  FROM
    `ltv.ltv_v1`) *2.5 + (
  SELECT
    AVG(historical_searches)
  FROM
    `ltv.ltv_v1`)
Union All
----multi-desktop
SELECT
 'Multi-Desktop', avg(total_clv) avg_LTV, sum(total_clv)*100 sum_LTV, count(distinct(client_id)) n, count(distinct(client_id))*100 population
FROM
  `ltv.ltv_v1`
WHERE
 country = 'US'
 AND
  sync_count_desktop_sum > 1
 AND 
-- Exclude outliers
 historical_searches < (
  SELECT
    STDDEV(historical_searches)
  FROM
    `ltv.ltv_v1`) *2.5 + (
  SELECT
    AVG(historical_searches)
  FROM
    `ltv.ltv_v1`)
Union ALL
    --Mobile Sync
SELECT
 'Mobile Sync', avg(total_clv) avg_LTV, sum(total_clv)*100 sum_LTV, count(distinct(client_id)) n, count(distinct(client_id))*100 population
FROM
  `ltv.ltv_v1`
WHERE
 country = 'US'
 AND
  sync_count_mobile_sum > 0
 AND 
-- Exclude outliers
 historical_searches < (
  SELECT
    STDDEV(historical_searches)
  FROM
    `ltv.ltv_v1`) *2.5 + (
  SELECT
    AVG(historical_searches)
  FROM
    `ltv.ltv_v1`)