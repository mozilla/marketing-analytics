WITH spendEfficiency AS(
SELECT
spend.date,
spend.vendorSpend,
downloads.downloads,
downloads.nonFXDownloads,
downloads.organicNonFXDownloads,
downloads.directNonFXDownloads,
downloads.nonFXDownloads-downloads.organicNonFXDownloads-downloads.directNonFXDownloads as mrktAttrDownloads,
installs.mozFunnelInstalls,
installs.darkFunnelInstalls,
installs.mozFunnelInstalls-installs.organicInstalls-installs.directInstalls as mrktAttrInstalls
FROM(
SELECT
  FORMAT_DATE('%Y%m%d', DATE) as date,
  SUM(VendorNetSpend) as vendorSpend
FROM
  `ga-mozilla-org-prod-001.fetch.fetch_deduped`
GROUP BY
  1) as spend

LEFT JOIN

(SELECT
  date as date,
  SUM(IF(downloads > 0,1,0)) as downloads,
  SUM(IF(downloads > 0 AND browser != 'Firefox',1,0)) as nonFXDownloads,
  SUM(IF(downloads > 0 AND browser != 'Firefox' AND medium = 'organic',1,0)) as organicNonFXDownloads,
  SUM(IF(downloads > 0 AND browser != 'Firefox' AND medium = '(none)',1,0)) as directNonFXDownloads
FROM (SELECT
  date AS date,
  fullVisitorId as visitorId,
  visitNumber as visitNumber,
  device.browser as browser,
  trafficSource.medium AS medium,
  SUM(IF (hits.eventInfo.eventAction = "Firefox Download",1,0)) as downloads
FROM
  `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
  UNNEST (hits) AS hits
WHERE
  _TABLE_SUFFIX >= '20171101'
  AND _TABLE_SUFFIX <= 'TODAY()-1'
  AND hits.type = 'EVENT'
  AND hits.eventInfo.eventCategory IS NOT NULL
  AND hits.eventInfo.eventLabel LIKE "Firefox for Desktop%"
GROUP BY
  1,2,3,4,5)
GROUP BY 1) as downloads

ON spend.date = downloads.date

LEFT JOIN
(SELECT
  submission_date_s3 AS date,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' THEN installs ELSE 0 END) as mozFunnelInstalls,
  SUM(CASE WHEN funnelOrigin = 'darkFunnel' THEN installs ELSE 0 END) as darkFunnelInstalls,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' AND mediumCleaned = 'organic' THEN installs ELSE 0 END) as organicInstalls,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' AND mediumCleaned = '(none)' AND sourceCleaned = '(direct)' THEN installs ELSE 0 END) as directInstalls
FROM
  `ga-mozilla-org-prod-001.telemetry.corpMetrics`
GROUP BY 1
ORDER BY 1
) as installs

ON spend.date = installs.date)

SELECT * from spendEfficiency
