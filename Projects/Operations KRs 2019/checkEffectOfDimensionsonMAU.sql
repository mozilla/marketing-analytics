WITH aggregated as (
SELECT
  count(DISTINCT client_id) as MAU
FROM
  `moz-fx-data-derived-datasets.telemetry.clients_daily_v6`
WHERE submission_date_s3 >= '2018-11-18' AND submission_date_s3 <= '2018-12-15'),

withCountryAdded as(
SELECT
  country,
  COUNT(DISTINCT client_id) as MAU
FROM
(SELECT
  country,
  client_id
FROM
  `moz-fx-data-derived-datasets.telemetry.clients_daily_v6`
WHERE submission_date_s3 >= '2018-11-18' AND submission_date_s3 <= '2018-12-15'
GROUP BY country, client_id)
GROUP By country),

withAttributionAdded as(
SELECT
  CONCAT(source, medium, campaign,content) as attribution,
  COUNT(DISTINCT client_id) as MAU
FROM
(SELECT
  attribution.source,
  attribution.medium,
  attribution.campaign,
  attribution.content,
  client_id
FROM
  `moz-fx-data-derived-datasets.telemetry.clients_daily_v6`
WHERE submission_date_s3 >= '2018-11-18' AND submission_date_s3 <= '2018-12-15'
GROUP BY 1,2,3,4,5)
GROUP By attribution)

SELECT
  'aggregated',
  SUM(aggregated.MAU) as MAU
FROM aggregated
UNION ALL
SELECT
  'withcountry',
  SUM(withCountryAdded.MAU) as MAU
FROM
  withCountryAdded
UNION ALL
SELECT
  'withAttribution',
  SUM(withAttributionAdded.MAU) as MAU
FROM
  withAttributionAdded