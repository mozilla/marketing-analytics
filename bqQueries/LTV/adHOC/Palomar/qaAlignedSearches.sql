 --Compares historical searches from LTV data set with sum of daily_searches from Palomar data set
  --Data is joined based on sha1 client_id from both tables
SELECT
  CASE
    WHEN ABS(historical_searches - sum_daily_searches) / historical_searches = 0 THEN '0'

WHEN ABS(historical_searches - sum_daily_searches) / historical_searches >= .01 AND ABS(historical_searches - sum_daily_searches) / historical_searches <.1 THEN '1-10'
    WHEN ABS(historical_searches - sum_daily_searches) / historical_searches >= .1 AND ABS(historical_searches - sum_daily_searches) / historical_searches <.2 THEN '10-20'
    WHEN ABS(historical_searches - sum_daily_searches) / historical_searches >= .2 AND ABS(historical_searches - sum_daily_searches) / historical_searches <.5 THEN '20-50'
    WHEN ABS(historical_searches - sum_daily_searches) / historical_searches >= .5
  AND ABS(historical_searches - sum_daily_searches) / historical_searches < .7 THEN '50-70'
    WHEN ABS(historical_searches - sum_daily_searches) / historical_searches >= .7 AND ABS(historical_searches - sum_daily_searches) / historical_searches < .9 THEN '70-90'
    WHEN ABS(historical_searches - sum_daily_searches) / historical_searches >= .9 THEN '>=90'
    ELSE '<20'
  END AS Pct_historical_searches_sum_daily_searches_delta,
  AVG(total_clv) avg_tLTV,
  AVG(historical_searches) avg_historical_searches,
  AVG(sum_daily_searches) avg_daily_searches,
  AVG(ABS(historical_searches - sum_daily_searches)) avg_daily_searches_delta,
  AVG(days_active) avg_days_active,
  AVG(days_dau) avg_days_dau,
  AVG(customer_age) avg_customer_age,
  COUNT(client_id) count_of_clients
FROM
  `ga-mozilla-org-prod-001.ltv.ltv_palomar`
  --Where user_status = 'Active'
 --Excludes clients that are older than palomar data set
 Where customer_age < 644
 and country like 'US'
GROUP BY
  1
  Order by 1 asc