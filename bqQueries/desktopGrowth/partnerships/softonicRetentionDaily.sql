-- Used to compare softonic retention rates to overall retention to support funding request - https://docs.google.com/spreadsheets/d/1ZYs88-g9R3-GQ1Ubl-wNdrohdxkDTBnEqLjaal7JaL0/edit#gid=0

WITH params as (
SELECT DATE("2019-03-01") as startDate, DATE("2019-03-01") as endDate
),

clientsDailyUsage as(
SELECT
  submission_date_s3,
  client_id,
  CASE WHEN distribution_id IS NULL THEN '' ELSE distribution_id END as distribution_id,
  country
FROM
  `moz-fx-data-derived-datasets.telemetry.clients_daily_v6`
WHERE
  submission_date_s3 >= (SELECT startDate from params)
  AND submission_date_s3 <= (SELECT endDate from params)
  AND country IN ('US', 'CA', 'DE', 'FR', 'GB')
),

newClients as (
SELECT
  client_id,
  min(submission) as installDate
FROM
  `moz-fx-data-derived-datasets.telemetry.telemetry_new_profile_parquet_v2`
WHERE
  submission >= DATE('2018-11-19')
  AND submission <= DATE('2018-12-31')
GROUP BY
  client_id),

cohorts as (
SELECT
  submission,
  metadata.geo_country as country,
  CASE WHEN environment.partner.distribution_id IS NULL THEN '' ELSE environment.partner.distribution_id END as distribution_id,
  COUNT(DISTINCT client_id) as installs
FROM
  `moz-fx-data-derived-datasets.telemetry.telemetry_new_profile_parquet_v2`
WHERE
  submission >= DATE('2018-11-19')
  AND submission <= DATE('2018-12-31')
  AND metaData.geo_country IN ('US', 'CA', 'DE', 'FR', 'GB')
GROUP BY
  submission, country, distribution_id ),

clientsUsageWithInstallDate as (
SELECT
clientsDailyUsage.submission_date_s3,
newClients.installDate,
clientsDailyUsage.client_id,
clientsDailyUsage.distribution_id,
clientsDailyUsage.country
FROM
  clientsDailyUsage
LEFT JOIN
  newClients
ON
  clientsDailyUsage.client_id = newClients.client_id
),

dailyRetention as (
SELECT
  submission_date_s3,
  installDate,
  DATE_DIFF(submission_date_s3, installDate, DAY) as daysRetained,
  clientsUsageWithInstallDate.distribution_id,
  clientsUsageWithInstallDate.country,
  cohorts.installs as cohort,
  COUNT(DISTINCT client_id) as DAU
FROM
  clientsUsageWithInstallDate
LEFT JOIN
  cohorts
ON
  clientsUsageWithInstallDate.installDate = cohorts.submission
  AND clientsUsageWithInstallDate.country = cohorts.country
  AND clientsUsageWithInstallDate.distribution_id = cohorts.distribution_id
GROUP BY
  submission_date_s3, installDate, daysRetained, distribution_id, country, cohort
)

SELECT
  submission_date_s3,
  installDate,
  daysRetained,
  CASE WHEN
    distribution_id = '' THEN 'nonPartnership' ELSE
    CASE WHEN distribution_ID IN ('softonic-002', 'softonic-003') THEN distribution_id ELSE
    'otherPartnerships' END END as distributionPartnership,
  country,
  SUM(cohort) as cohort,
  SUM(DAU) as DAU
from dailyRetention
GROUP BY
  submission_date_s3, installDate, daysRetained, distributionPartnership, country