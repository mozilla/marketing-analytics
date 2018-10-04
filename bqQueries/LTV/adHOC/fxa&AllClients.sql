--Excludes outliers where variance > 2.5 standard deviations from the mean
SELECT
 'Is Default browser' as segment, country, count(distinct(client_id)) n, count(distinct(client_id))*100 population
FROM
  `ltv.ltv_v1`
WHERE
 country in ('DE','US')
 AND 
 is_default_browser = 't'
 AND
 profile_creation_date > '20170401'AND
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
GROUP By
Country
Union ALL
SELECT
 'All clients', country, count(distinct(client_id)) n, count(distinct(client_id))*100 population
FROM
  `ltv.ltv_v1`
WHERE
 country in ('DE','US')
  AND
 profile_creation_date > '20170401'
 And 
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
Group by COuntry
------worldwide
Union all
SELECT
 'Is Default browser', 'World-wide', count(distinct(client_id)) n, count(distinct(client_id))*100 population
FROM
  `ltv.ltv_v1`
WHERE

 is_default_browser = 't'
 AND
 profile_creation_date > '20170401' AND
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
SELECT
 'All clients', 'World-wide', count(distinct(client_id)) n, count(distinct(client_id))*100 population
FROM
  `ltv.ltv_v1`
WHERE
 
 profile_creation_date > '20170401' AND
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