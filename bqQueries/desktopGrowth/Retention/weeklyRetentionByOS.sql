WITH params as (
SELECT DATE("2019-05-01") as startDate, DATE("2019-09-28") as endDate, DATE("2019-05-01") as installStartDate, DATE("2019-09-28") as installEndDate),

installs as (
SELECT
  -- Using MIN to reduce number of -ve days / weeks retained
  MIN(submission) as installDate,
  client_id,
  CASE WHEN environment.system.os.name IN ('Darwin', 'Windows_NT', 'Linux') THEN environment.system.os.name ELSE 'other' END as os
FROM
  `moz-fx-data-derived-datasets.telemetry_derived.telemetry_new_profile_parquet_v2`
WHERE
  submission >= (SELECT installStartDate FROM params)
  AND submission <= (SELECT installEndDate FROM params)
GROUP BY
  client_id, os),

cohorts as (
SELECT
  FORMAT_DATE("%U", installDate) as installWeek,
  MIN(installDate) as installWeekStart,
  COUNT(DISTINCT client_id) as installs,
  os
FROM
  installs
GROUP BY
  installWeek, os),

retainedUsers as (
SELECT
  submission_date as usageDate,
  client_id,
  CASE WHEN os IN ('Darwin', 'Windows_NT', 'Linux') THEN os ELSE 'other' END as os
FROM
  `moz-fx-data-derived-datasets.telemetry_derived.clients_daily_v6`
WHERE
  submission_date >= (SELECT startDate FROM params)
  AND submission_date <= (SELECT endDate FROM params)
GROUP BY
  usageDate, client_id, os),

retainedUsersWithInstallDate as (
SELECT
  retainedUsers.usageDate,
  retainedUsers.client_id,
  installs.installDate,
  retainedUsers.os
FROm
  retainedUsers
LEFT JOIN
  installs
ON
  retainedUsers.client_id = installs.client_id
  AND retainedUsers.os = installs.os),

retainedUsersSummary as (
SELECT
  FORMAT_DATE("%U", installDate) as installWeek,
  FORMAT_DATE("%U", usageDate) as usageWeek,
  CAST(FORMAT_DATE("%U", usageDate) AS INT64) - CAST(FORMAT_DATE("%U", installDate) AS INT64) as weeksRetained,
  os,
  COUNT(DISTINCT client_id) as clients
FROM
  retainedUsersWithInstallDate
GROUP BY
  1,2,3,4
),

retention as (
SELECT
  cohorts.installWeek,
  cohorts.installWeekStart,
  cohorts.installs as cohort,
  cohorts.os as os,
  retainedUsersSummary.usageWeek,
  retainedUsersSummary.weeksRetained,
  retainedUsersSummary.clients as retained
FROM
  retainedUsersSummary
LEFT JOIN
  cohorts
ON
  retainedUsersSummary.installWeek = cohorts.installWeek
  AND retainedUsersSummary.os = cohorts.os
WHERE
  weeksRetained >= 0
GROUP BY
  cohort, os, installWeek, usageWeek, weeksRetained, installWeekStart, retained)


SELECT
  installWeek,
  installWeekStart,
  os,
  usageWeek,
  weeksRetained,
  cohort,
  SUM(retained) as retained
FROM retention
GROUP BY
  installWeek,
  installWeekStart,
  os,
  usageWeek,
  weeksRetained,
  cohort
ORDER BY
  installWeek