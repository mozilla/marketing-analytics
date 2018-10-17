-- Calculate install rate for mozilla.org funnel
-- Downloads from GA, installs from telemetry

WITH acqByDate AS(
SELECT
downloads.date,
downloads.downloads as downloads,
downloads.nonFXDownloads as nonFXDownloads,
downloads.windowsDownloads as windowsDownloads,
downloads.windowsNonFXDownloads as windowsNonFXDownloads,
SUM(installs.installs) as totalInstalls,
SUM(CASE WHEN installs.funnelOrigin = 'mozFunnel' THEN installs.installs ELSE 0 END) as mozFunnelInstalls,
SUM(CASE WHEN installs.funnelOrigin = 'darkFunnel' THEN installs.installs ELSE 0 END) as darkFunnelInstalls
FROM(

-- Calculate downloads
(SELECT
  date as date,
  SUM(IF(downloads > 0,1,0)) as downloads,
  SUM(IF(downloads > 0 AND browser != 'Firefox',1,0)) as nonFXDownloads,
  SUM(IF(downloads > 0 AND operatingSystem = 'Windows',1,0)) as windowsDownloads,
  SUM(IF(downloads > 0 AND browser != 'Firefox' AND operatingSystem = 'Windows' ,1,0)) as windowsNonFXDownloads
FROM (SELECT
  date AS date,
  fullVisitorId as visitorId,
  visitNumber as visitNumber,
  trafficSource.source as source,
  device.browser as browser,
  device.operatingSystem as operatingSystem,
  SUM(IF (hits.eventInfo.eventAction = "Firefox Download",1,0)) as downloads
FROM
  `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
  UNNEST (hits) AS hits
WHERE
  _TABLE_SUFFIX >= '20170101'
  AND hits.type = 'EVENT'
  AND hits.eventInfo.eventCategory IS NOT NULL
  AND hits.eventInfo.eventLabel LIKE "Firefox for Desktop%"
GROUP BY
  1,2,3,4,5,6)
GROUP BY 1
ORDER BY 1) as downloads

LEFT JOIN
-- Calculate Installs By Funnel
(SELECT
  submission_date_s3 AS date,
  funnelOrigin,
  SUM(installs) AS installs
FROM
  `ga-mozilla-org-prod-001.telemetry.corpMetrics`
GROUP BY 1,2
ORDER BY 1,2) as installs

ON downloads.date = installs.date)
GROUP BY 1,2,3,4,5)


SELECT
*,
CASE WHEN windowsNonFXDownloads <= 0 THEN 0 ELSE mozFunnelInstalls/windowsNonFXDownloads END as installRate
FROM acqByDate
WHERE date >= '20170101'
ORDER BY 1