WITH params as (
SELECT DATE("2019-09-01") as startDate, DATE("2019-09-14") as endDate, DATE("2019-09-01") as installStartDate, DATE("2019-09-07") as installEndDate),

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
  FORMAT_DATE("%U", installDate) as installWeek,
  MIN(installDate) as installWeekStart,
  COUNT(DISTINCT client_id) as installs
FROM
  installs
GROUP BY
  installWeek),

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
  FORMAT_DATE("%U", installDate) as installWeek,
  FORMAT_DATE("%U", usageDate) as usageWeek,
  CAST(FORMAT_DATE("%U", usageDate) AS INT64) - CAST(FORMAT_DATE("%U", installDate) AS INT64) as weeksRetained,
  COUNT(DISTINCT client_id) as clients
FROM
  retainedUsersWithInstallDate
GROUP BY
  installDate, usageDate
),

retention as (
SELECT
  cohorts.installWeek,
  cohorts.installWeekStart,
  cohorts.installs as cohort,
  retainedUsersSummary.usageWeek,
  retainedUsersSummary.weeksRetained,
  SUM(retainedUsersSummary.clients) as retained
FROM
  retainedUsersSummary
LEFT JOIN
  cohorts
ON
  retainedUsersSummary.installWeek = cohorts.installWeek
WHERE
  weeksRetained >= 0
GROUP BY
  cohort, installWeek, usageWeek, weeksRetained, installWeekStart)

SELECT
  installWeek,
  installWeekStart,
  usageWeek,
  weeksRetained,
  cohort,
  SUM(retained) as retained
FROM retention
GROUP BY
  installWeek,
  installWeekStart,
  usageWeek,
  weeksRetained,
  cohort
ORDER BY
  installWeek