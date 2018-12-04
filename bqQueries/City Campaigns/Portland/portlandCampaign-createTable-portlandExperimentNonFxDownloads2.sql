WITH data as(
SELECT
CY.CYDate,
CY.PYDate,
CY.city,
CY.state,
DMATable.DMA,
IFNULL(CY.CYDownloads,0) as CYDownloads,
IFNULL(PY.PYDownloads,0) as PYDownloads,
IFNULL(PY.PYDownloadsSpike,0) as PYDownloadsSpike,
IFNULL(PY.PYDownloadsSmoothed,0) as PYDownloadsSmoothed,
IFNULL(spend.vendorNetSpend,0) as vendorNetSpend
FROM(
(SELECT
PARSE_DATE('%Y%m%d', downloadDate) as CYDate,
city,
state,
DATE_SUB(PARSE_DATE('%Y%m%d', downloadDate), INTERVAL 364 DAY) as PYDate, -- using 364 to line up days of the week i.e. sunday to sunday
IFNULL(SUM(IF(desktop_downloads > 0 ,1,0)),0) AS CYDownloads
FROM (
SELECT
  fullVisitorId,
  visitNumber,
  geoNetwork.city as city,
  geoNetwork.region as state,
  date AS downloadDate,
  SUM(IF(hits.eventInfo.eventAction = "Firefox Download" AND hits.eventInfo.eventLabel LIKE "Firefox for Desktop%",1,0)) AS desktop_downloads
FROM `ga-mozilla-org-prod-001.65789850.ga_sessions_*` ,
  UNNEST (hits) as hits
WHERE
  _TABLE_SUFFIX >= '20170101'
  AND _TABLE_SUFFIX <= 'TODAY()-1'
  AND hits.type="EVENT"
  AND hits.eventInfo.eventCategory IS NOT NULL
  AND geoNetwork.Region IN ('Oregon','Washington','Wisconsin')
GROUP BY
  fullVisitorId,
  visitNumber,
  city,
  state,
  downloadDate)
WHERE  PARSE_DATE('%Y%m%d', downloadDate) > '2017-12-31'
GROUP BY
1,2,3,4
ORDER BY 1 asc) as CY

LEFT JOIN

(
SELECT
  PARSE_DATE('%Y%m%d', downloads.date) as PYDate,
  downloads.city as city,
  downloads.state as state,
  DATE_ADD(PARSE_DATE('%Y%m%d', downloads.date), INTERVAL 364 DAY) as CYDate, -- using 364 to line up days of the week i.e. sunday to sunday
  SUM(IF(downloads.downloads > 0,1,0)) as PYDownloads,
  SUM(IF(downloads.downloads > 0 AND browser != 'Firefox',1,0)) as PYNonFXDownloads,
  IFNULL(SUM(CASE WHEN source = '(direct)'  AND browser = 'Firefox' AND browserVersion LIKE '52%' AND operatingSystem = 'Windows' AND landingPage.landingPage LIKE '%/firefox/new%' AND landingPage.landingPage NOT LIKE '%firefox/new/?scene=2%' THEN IF(downloads.downloads > 0,1,0) END),0) as PYDownloadsSpike,
  SUM(IF(downloads.downloads > 0,1,0)) - IFNULL(SUM(CASE WHEN source = '(direct)'  AND browser = 'Firefox' AND browserVersion LIKE '52%' AND operatingSystem = 'Windows' AND landingPage.landingPage LIKE '%/firefox/new%' AND landingPage.landingPage NOT LIKE '%firefox/new/?scene=2%' THEN IF(downloads.downloads > 0,1,0) END),0) as PYDownloadsSmoothed
FROM (
-- Joined Table: joins specific visitID / visitorID information with Landing page to find landing page of the visitID / visitorID
(
-- select activity by visitorID, visitID, visitNumber across multiple dimensions
SELECT
  date AS date,
  fullVisitorId AS fullVisitorId,
  visitID AS visitID,
  visitNumber AS visitNumber,
  trafficSource.source AS source,
  device.browser AS browser,
  device.browserVersion AS browserVersion,
  device.operatingSystem AS operatingSystem,
  geoNetwork.country AS country,
  geoNetwork.city AS city,
  geoNetwork.region AS state,
  SUM(IF (hits.eventInfo.eventAction = "Firefox Download" AND hits.eventInfo.eventLabel LIKE "Firefox for Desktop%",1,0)) as downloads
FROM
  `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
  UNNEST (hits) AS hits
WHERE
  _TABLE_SUFFIX >= '20170101'
  AND _TABLE_SUFFIX <= '20171231'
  AND hits.type = 'EVENT'
  AND hits.eventInfo.eventCategory IS NOT NULL
  AND geoNetwork.Region IN ('Oregon','Washington','Wisconsin')
GROUP BY
  1,2,3,4,5,6,7,8,9,10,11) as downloads

LEFT JOIN
(
-- find landing page for specific visitID, visitorID
SELECT
date,
fullVisitorID,
visitID,
landingPage,
COUNT(DISTINCT(CONCAT(fullVisitorID,CAST(visitId as STRING)))) as sessions
FROM
(SELECT
date,
fullVisitorId,
visitId,
hits.hitNumber AS hitNumber,
hits.type,
hits.page.pagePath as landingPage
FROM
  `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
  UNNEST (hits) AS hits
WHERE
  _TABLE_SUFFIX >= '20170101'
  AND _TABLE_SUFFIX <= '20171231'
  AND hits.type = 'PAGE'
  AND hitNumber = 1
GROUP BY
  1,2,3,4,5,6)
GROUP BY 1,2,3,4) as landingPage

ON downloads.date = landingPage.date AND downloads.fullVisitorId = landingPage.fullVisitorId AND downloads.visitID = landingPage.visitID)

GROUP BY 1,2,3,4) as PY

ON CY.CYDate = PY.CYDate AND CY.city = PY.city AND CY.state = PY.state

LEFT JOIN

(SELECT
  city,
  state,
  DMA
FROM
  `ga-mozilla-org-prod-001.65789850.portlandMadisonDMA`
GROUP BY
  1,2,3) as DMATable

ON CY.city = DMATable.city AND CY.state = DMATable.state

LEFT JOIN

(SELECT
  Date,
  'Portland' as city,
  ROUND(SUM(VendorNetSpend),2) AS vendorNetSpend
FROM
  `ga-mozilla-org-prod-001.fetch.fetch_deduped`
WHERE
  campaign = 'City Portland 2018'
  AND Date >= '2018-01-01'
GROUP BY 1,2
ORDER BY 1) as spend

ON CY.CYDate = spend.date AND CY.city = spend.city))

SELECT * FROM data
WHERE DMA IS NOT NULL
ORDER BY 1