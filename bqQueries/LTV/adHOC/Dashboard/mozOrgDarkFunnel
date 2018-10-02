--Excludes outliers where variance > 2.5 standard deviations from the mean


SELECT
 avg(total_clv) avg_tLTV, avg(predicted_clv_12_months) avg_pLTV, avg(historical_clv) avg_hLTV, sum(total_clv)*100 sum_LTV, count(distinct(client_id)) n, count(distinct(client_id))*100 population, attribution_site
FROM
  `ltv.ltv_v1_backfilled`
WHERE
 country = 'US'
 AND 
 customer_age < 155
 AND
-- Exclude outliers
 historical_searches < (
  SELECT
    STDDEV(historical_searches)
  FROM
    `ltv.ltv_v1_backfilled`) *2.5 + (
  SELECT
    AVG(historical_searches)
  FROM
    `ltv.ltv_v1_backfilled`)
Group by attribution_site