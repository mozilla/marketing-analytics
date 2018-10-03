--LTV by country excluding outliers and clients without a country 

SELECT
  country,
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
And attribution_site is null
And user_status = "Active"
GROUP BY
  country
ORDER BY n desc