--Marketing Attributable Clients by country
--Includes traffic that belongs to a known marketing attributable medium (cpc, paidsearch, email, snippet, video, native, display, social
--Excludes outliers where variance > 2.5 standard deviations from the mean

SELECT
  avg(total_clv) avg_LTV, sum(total_clv)*100 sum_LTV, avg(predicted_clv_12_months) avg_pltv, count(distinct(client_id)) n, count(distinct(client_id))*100 population
FROM
  `ltv.ltv_v1`
WHERE
 attribution_site = 'www_mozilla_org'
 AND
max_activity_date >= '2018-07-10'
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


