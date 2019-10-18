WITH params as (
SELECT DATE("2019-09-01") as startDate, DATE("2019-09-02") as endDate, DATE("2019-09-01") as installStartDate, DATE("2019-09-02") as installEndDate),

installs as (
SELECT
  -- Using MIN to reduce number of -ve days / weeks retained
  MIN(submission) as installDate,
  client_id
FROM
  `moz-fx-data-derived-datasets.telemetry_derived.telemetry_new_profile_parquet_v2`
WHERE
  submission >= (SELECT installStartDate FROM params)
  AND submission <= (SELECT installEndDate FROM params)
GROUP BY
  client_id),

cohorts as (
SELECT
  installDate,
  COUNT(DISTINCT client_id) as installs
FROM
  installs
GROUP BY
  installDate),

retainedUsers as (
SELECT
  submission_date as usageDate,
  client_id
FROM
  `moz-fx-data-derived-datasets.telemetry_derived.clients_daily_v6`
WHERE
  submission_date >= (SELECT startDate FROM params)
  AND submission_date <= (SELECT endDate FROM params)
GROUP BY
  usageDate, client_id),

retainedUsersWithInstallDate as (
SELECT
  retainedUsers.usageDate,
  retainedUsers.client_id,
  installs.installDate
FROm
  retainedUsers
LEFT JOIN
  installs
ON
  retainedUsers.client_id = installs.client_id),

retainedUsersSummary as (
SELECT
  installDate,
  usageDate,
  DATE_DIFF(usageDate, installDate, day) as daysRetained,
  COUNT(DISTINCT client_id) as clients
FROM
  retainedUsersWithInstallDate
GROUP BY
  installDate, usageDate
ORDER BY
  daysRetained),

retention as (
SELECT
  cohorts.installDate,
  SUM(cohorts.installs) as cohort,
  retainedUsersSummary.usageDate,
  retainedUsersSummary.daysRetained,
  SUM(retainedUsersSummary.clients) as retained
FROM
  retainedUsersSummary
LEFT JOIN
  cohorts
ON
  retainedUsersSummary.installDate = cohorts.installDate
WHERE
  daysRetained >= 0
GROUP BY
  installDate, usageDate, daysRetained)

SELECT
  installDate,
  usageDate,
  daysRetained,
  SUM(cohort) as cohort,
  SUM(retained) as retained
FROM retention
GROUP BY
  installDate,
  usageDate,
  daysRetained