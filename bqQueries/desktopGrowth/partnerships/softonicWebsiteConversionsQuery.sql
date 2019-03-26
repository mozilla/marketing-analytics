-- Used for weekly softonic reporting - https://docs.google.com/spreadsheets/d/1bd6BCnFUQf3jjoauwxCPzj5k_34Lk8XbnqJVfRJQ7eU/edit#gid=1020253749

SELECT
date,
medium,
source,
SUM(sessions) as sessions,
SUM(nonFXSessions) as nonFXSessions,
SUM(downloads) as downloads,
SUM(nonFXDownloads) as nonFXDownloads
FROM
  `ga-mozilla-org-prod-001.desktop.website_metrics_*`
WHERE
  date >= "2019-03-18"
  AND date <= "2019-03-23"
  AND medium IN ('affiliate')
GROUP BY date,medium, source
ORDER BY date