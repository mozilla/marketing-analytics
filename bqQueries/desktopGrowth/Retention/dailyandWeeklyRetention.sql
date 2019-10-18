WITH params as (
SELECT DATE("2019-05-17") as startDate, DATE("2019-09-28") as endDate, DATE("2019-05-17") as installStartDate, DATE("2019-09-28") as installEndDate
),

clientsDailyUsage as(
SELECT
  submission_date,
  CASE
    WHEN attribution.source IS NULL AND attribution.medium IS NULL AND attribution.campaign IS NULL AND attribution.content IS NULL AND distribution_id IS NULL THEN 'darkFunnel'
    WHEN distribution_id IS NOT NULL THEN 'partnerships' ELSE
    'mozFunnel' END as funnelOrigin,
  CASE WHEN distribution_id IN ('softonic-003', 'softonic-005','softonic-006', 'softonic-007','softonic-008','softonic-009','softonic-010','softonic-011','softonic-012') THEn distribution_id ELSE 'other' END as distribution_id,
  client_id
FROM
  `moz-fx-data-derived-datasets.telemetry_derived.clients_daily_v6`
WHERE
  submission_date >= (SELECT startDate from params)
  AND submission_date <= (SELECT endDate from params)
GROUP BY
  submission_date, client_id, funnelOrigin, distribution_id
),

newClients as (
SELECT
client_id,
CASE
  WHEN source = 'unknown' AND medium = 'unknown' AND campaign = 'unknown' AND content = 'unknown' AND distribution_id = '' THEN 'darkFunnel'
  WHEN distribution_id != '' THEN 'partnerships' ELSE
  'mozFunnel' END as funnelOrigin,
CASE WHEN distribution_id IN ('softonic-003', 'softonic-005','softonic-006', 'softonic-007','softonic-008','softonic-009','softonic-010','softonic-011','softonic-012') THEn distribution_id ELSE 'other' END as distribution_id,
min(submission) as installDate
FROM
`moz-fx-data-derived-datasets.analysis.gkabbz_newProfileCountModifications_*`
WHERE
submission >= (SELECT installStartDate from params)
AND submission <= (SELECT installEndDate from params)
GROUP BY
client_id, funnelOrigin, distribution_id),

cohorts as (
SELECT
submission,
CASE
  WHEN source = 'unknown' AND medium = 'unknown' AND campaign = 'unknown' AND content = 'unknown' AND distribution_id = '' THEN 'darkFunnel'
  WHEN distribution_id != '' THEN 'partnerships' ELSE
  'mozFunnel' END as funnelOrigin,
CASE WHEN distribution_id IN ('softonic-003', 'softonic-005','softonic-006', 'softonic-007','softonic-008','softonic-009','softonic-010','softonic-011','softonic-012') THEn distribution_id ELSE 'other' END as distribution_id,
COUNT(DISTINCT client_id) as installs
FROM
`moz-fx-data-derived-datasets.analysis.gkabbz_newProfileCountModifications_*`
WHERE
submission >= (SELECT installStartDate from params)
AND submission <= (SELECT installEndDate from params)
GROUP BY
submission, funnelOrigin, distribution_id),

cohortsWeekly as (
SELECT
  FORMAT_DATE("%U", submission) as week,
  MIN(submission) as weekStart,
  funnelOrigin,
  distribution_id,
  SUM(installs) as installs
FROM
  cohorts
GROUP BY
  week, funnelOrigin, distribution_id),

clientsUsageWithInstallDate as (
SELECT
clientsDailyUsage.submission_date,
newClients.installDate,
clientsDailyUsage.funnelOrigin,
clientsDailyUsage.distribution_id,
clientsDailyUsage.client_id
FROM
clientsDailyUsage
LEFT JOIN
newClients
ON
clientsDailyUsage.client_id = newClients.client_id
),

dailyRetention as (
SELECT
submission_date,
installDate,
DATE_DIFF(submission_date, installDate, DAY) as daysRetained,
cohorts.installs as cohort,
COUNT(DISTINCT client_id) as DAU
FROM
clientsUsageWithInstallDate
LEFT JOIN
cohorts
ON
clientsUsageWithInstallDate.installDate = cohorts.submission
GROUP BY
submission_date, installDate, daysRetained,cohort
),

weeklyRetention as (
SELECT
  FORMAT_DATE("%U", submission_date) as weekUsed,
  FORMAT_DATE("%U", installDate) as weekInstalled,
  MIN(installDate) as weekStart,
  clientsUsageWithInstallDate.funnelOrigin,
  clientsUsageWithInstallDate.distribution_id,
  CAST(FORMAT_DATE("%U", submission_date) AS INT64) - CAST(FORMAT_DATE("%U", installDate) AS INT64) as weeksRetained,
  cohortsWeekly.installs as cohort,
  COUNT(DISTINCT client_id) as WAU
FROM
  clientsUsageWithInstallDate
LEFT JOIN
  cohortsWeekly
ON
  FORMAT_DATE("%U", clientsUsageWithInstallDate.installDate) = cohortsWeekly.week
  AND clientsUsageWithInstallDate.funnelOrigin = cohortsWeekly.funnelOrigin
  AND clientsUsageWithInstallDate.distribution_id = cohortsWeekly.distribution_id
GROUP BY
  weekUsed, weekInstalled,weeksRetained, cohort, funnelOrigin, distribution_id)

SELECT
funnelOrigin,
distribution_id,
weekStart,
weekInstalled,
weeksRetained,
SUM(cohort) as cohort,
SUM(WAU) as WAU,
SAFE_DIVIDE(SUM(WAU), SUM(cohort)) as weeklyRetentionRate
from weeklyRetention
WHERE weekStart IS NOT NULL
GROUP BY
weekStart,weekInstalled, weeksRetained, funnelOrigin, distribution_id
ORDER BY
funnelOrigin, distribution_id, weekStart, weeksRetained