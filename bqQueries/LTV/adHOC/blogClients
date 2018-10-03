SELECT

    campaign,
    AVG(total_clv) avg_tLTV,
    SUM(total_clv)*100 sum_tLVT,
    AVG(predicted_clv_12_months) avg_pLTV,
    SUM(predicted_clv_12_months) sum_pLTV,
    COUNT(DISTINCT(client_id)) n,
    COUNT(DISTINCT(client_id))*100 population
  FROM
    `ltv.ltv_v1`
  Where
    country like 'US'
   AND
    campaign like '%blog%'
    AND
    historical_searches < (
  SELECT
    STDDEV(historical_searches)
  FROM
    `ltv.ltv_v1`) *2.5 + (
  SELECT
    AVG(historical_searches)
  FROM
    `ltv.ltv_v1`)
  GROUP BY
    campaign
  ORDER BY
    n DESC