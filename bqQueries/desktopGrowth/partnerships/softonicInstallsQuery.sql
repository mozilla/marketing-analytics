-- Used for weekly softonic reporting - https://docs.google.com/spreadsheets/d/1bd6BCnFUQf3jjoauwxCPzj5k_34Lk8XbnqJVfRJQ7eU/edit#gid=1020253749

SELECT
submission as date,
distribution_id as distroIDorMedium,
'softonic' as source,
country,
SUM(installs) as installs
FROM
  `ga-mozilla-org-prod-001.telemetry.desktopTelemetryDrop`
WHERE distribution_id LIKE "softonic-005"
AND submission >= '2019-03-19'
GROUP BY submission, distribution_id, source, country


UNION ALL

SELECT
submission,
medium,
source,
country,
SUM(installs) as installs
FROM
  `ga-mozilla-org-prod-001.telemetry.desktopTelemetryDrop`
WHERE medium LIKE "affiliate%"
AND submission >= '2019-03-19'
GROUP BY submission, medium, source, country
Order by distroIDorMedium, date