--Marketing Attributable Clients by country
--Includes traffic that belongs to a known marketing attributable medium (cpc, paidsearch, email, snippet, video, native, display, social
--Excludes outliers where variance > 2.5 standard deviations from the mean


SELECT
  avg(total_clv) avg_LTV, sum(total_clv)*100 sum_LTV, count(distinct(client_id)) n, count(distinct(client_id))*100 population
FROM
  `ltv.ltv_v1`
WHERE
 country = 'DE'
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
 --medium is a known paid medium
  And
 (medium IN('cpc',
    'paidsearch',
    'email',
    'snippet',
    'video',
    'native',
    'display',
    'social')
  OR ((medium LIKE 'referral')
  --Referrals match a known marketing attributable Source, medium, campaign
  AND CONCAT(medium, source, campaign) 
    IN (
    SELECT
       distinct CONCAT(medium, source, campaign)
    FROM
       `ltv.ltv_v1`
    WHERE
       medium = 'referral'
       AND campaign <> '%2528not%2Bset%2529')))