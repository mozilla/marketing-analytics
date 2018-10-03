--LTV by country excluding outliers and clients without a country 

SELECT
  AVG(historical_searches) avg_hSearches,
  AVG(historical_clv) avg_hLTV,
  AVG(predicted_clv_12_months) avg_pLTV,
  AVG(total_clv) avg_tLTV,
  SUM(total_clv) sum_tLTVx100,
  COUNT(distinct(client_id)) n,
  COUNT(distinct(client_id)) * 100 est_population
FROM
  `ltv.ltv_v1` 
WHERE
--Remove outliers
  historical_searches < (
  SELECT
    STDDEV(historical_searches)
  FROM
    `ltv.ltv_v1`) *5 + (
  SELECT
    AVG(historical_searches)
  FROM
    `ltv.ltv_v1`)
AND country is not null

ORDER BY n desc