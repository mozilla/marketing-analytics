
  SELECT
    case when medium = 'referral'
      AND campaign = '%2528not%2Bset%2529'
      AND (source like '%google%' OR source like '%www.bing%' OR source Like '%search.yahoo%' or source like
                    '%yandex%' or source like '%seznam%' or source like '%baidu%' OR source like '%naver%' OR source like'%ask%')
    then 'organic search'
    When medium in('%2528none%2529','%2528direct%2529') Then 'direct'
    else medium end clean_medium,
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
  GROUP BY
    clean_medium
  ORDER BY
    n DESC