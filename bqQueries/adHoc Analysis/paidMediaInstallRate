-- Calculate install rate for mozilla.org funnel
-- Downloads from GA, installs from telemetry

WITH installRate AS(
SELECT
downloads.date,
downloads.medium,
downloads.downloads,
downloads.nonFXDownloads,
installs.installs
--installs.installs/downloads.nonFXDownloads as installRate
FROM(

-- Calculate downloads for paid media channels (display, banner, ppc, cpc, paidsearch)
(SELECT
  date as date,
  medium,
  SUM(IF(downloads > 0,1,0)) as downloads,
  SUM(IF(downloads > 0 AND browser != 'Firefox',1,0)) as nonFXDownloads
FROM (SELECT
  date AS date,
  fullVisitorId as visitorId,
  visitNumber as visitNumber,
  trafficSource.medium as medium,
  device.browser as browser,
  SUM(IF (hits.eventInfo.eventAction = "Firefox Download",1,0)) as downloads
FROM
  `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
  UNNEST (hits) AS hits
WHERE
  _TABLE_SUFFIX >= '20171201'
  AND hits.type = 'EVENT'
  AND hits.eventInfo.eventCategory IS NOT NULL
  AND hits.eventInfo.eventLabel LIKE "Firefox for Desktop%"
  AND trafficSource.medium IN ('display', 'banner', 'ppc', 'cpc', 'paidsearch')
GROUP BY
  1,2,3,4,5)
GROUP BY 1,2
ORDER BY 1) as downloads

LEFT JOIN
-- Calculate Installs for paid media channels (display, banner, ppc, cpc, paidsearch)
(SELECT
  submission_date_s3 AS date,
  mediumCleaned AS medium,
  SUM(installs) AS installs
FROM
  `ga-mozilla-org-prod-001.telemetry.corpMetrics`
WHERE
  funnelOrigin = 'mozFunnel'
  AND mediumCleaned IN ('display','banner', 'cpc','ppc','paidsearch')
  AND submission_date_s3 >= '20171201'
GROUP BY 1, 2
ORDER BY 1) as installs

ON downloads.date = installs.date
AND downloads.medium = installs.medium))

SELECT
FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS date,
medium,
SUM(downloads) as downloads,
SUM(nonFXDownloads) as nonFXDownloads,
SUM(installs) as installs
FROM installRate
GROUP BY 1,2
ORDER BY 1