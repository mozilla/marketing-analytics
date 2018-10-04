SELECT
  country,
  AVG(total_clv) avg_tLTV,
  SUM(total_clv) sum_tLTVx100,
  COUNT(client_id) n,
  COUNT(client_id)*100 population
FROM
  `ltv.ltv_v1c`
WHERE
  historical_searches < (
  SELECT
    STDDEV(historical_searches)
  FROM
    `ltv.ltv_v1c`) *2.5 + (
  SELECT
    AVG(historical_searches)
  FROM
    `ltv.ltv_v1c`)
  AND country like 'US'
GROUP BY
  country
ORDER BY n desc