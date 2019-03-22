WITH clientsLastSeen as (
SELECT
  submission_date,
  last_seen_date,
  client_id,
  attribution.source,
  attribution.medium,
  attribution.campaign,
  attribution.content,
  country,
  distribution_id
FROM
  `moz-fx-data-derived-datasets.analysis.clients_last_seen_v1`
WHERE
  submission_date = '2018-12-15'
GROUP BY
  submission_date,
  last_seen_date,
  client_id,
  source,
  medium,
  campaign,
  content,
  country,
  distribution_id),

newClients as(
SELECT
  client_id,
  min(submission) as installDate
FROM
  `moz-fx-data-derived-datasets.telemetry.telemetry_new_profile_parquet_v2`
WHERE
  submission >= DATE('2017-01-01')
GROUP BY
  client_id),


clientsWithInstallDate as (
  SELECT
  clientsLastSeen.submission_date,
  clientsLastSeen.source,
  clientsLastSeen.medium,
  clientsLastSeen.campaign,
  clientsLastSeen.content,
  clientsLastSeen.country,
  newClients.installDate,
  COUNT(*) as mau,
  COUNTIF(last_seen_date = submission_date) as dau
FROM
  clientsLastSeen
LEFT JOIN
  newClients
ON
  clientsLastSeen.client_id = newClients.client_id
GROUP BY
  submission_date, source, medium, campaign, content, country, installdate)

SELECT
submission_date,
installDate,
CASE WHEN EXTRACT(YEAR FROM installDate) = 2018 THEN 'new2018' ELSE 'existing' END as acqSegment,
CASE
    WHEN source IS NULL
    AND medium IS NULL
    AND campaign IS NULL
    AND content IS NULL THEN 'darkFunnel' ELSE 'mozFunnel' END as funnelOrigin,
country,
SUM(mau) as MAU
FROM clientsWithInstallDate
GROUP BY submission_date, installDate, acqSegment, country, funnelOrigin