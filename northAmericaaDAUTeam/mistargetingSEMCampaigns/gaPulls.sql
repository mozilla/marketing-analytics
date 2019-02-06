WITH alldata as(
SELECT
sessionsTable.month,
sessionsTable.country,
sessionsTable.medium,
sessionsTable.source,
sessionsTable.campaign,
sessionsTable.adContent,
SUM(sessionsTable.totalSessions) as totalSessions,
SUM(sessionsTable.nonFxSessions) as nonFxSessions,
SUM(hitsTable.downloads) as downloads,
SUM(hitsTable.nonFXDownloads) as nonFXDownloads,
fetchSpend.costPerClick as costPerTrackedClick

FROM(
-- session level metrics --
(SELECT
month,
country,
medium,
source,
campaign,
adContent,
SUM(siteSessions) as totalSessions,
SUM(CASE WHEN browser != 'Firefox' THEN siteSessions ELSE 0 END) as nonFxSessions
FROM
(SELECT
  FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d",date)) AS month,
  trafficSource.medium,
  trafficSource.source,
  trafficSource.campaign,
  trafficSource.adContent,
  geoNetwork.country,
  device.browser AS browser,
  SUM(totals.visits) AS siteSessions
FROM
  `ga-mozilla-org-prod-001.65789850.ga_sessions_*`
WHERE
  _TABLE_SUFFIX >= '20180101'
  AND _TABLE_SUFFIX <= '20181231'
GROUP BY 1,2,3,4,5,6,7)
GROUP BY 1,2,3,4,5,6
) AS sessionsTable

LEFT JOIN

-- hits based metrics --
(SELECT
month,
country,
medium,
source,
campaign,
adContent,
SUM(IF(downloads > 0,1,0)) as downloads,
SUM(IF(downloads > 0 AND browser != 'Firefox',1,0)) as nonFXDownloads
FROM
(SELECT
  FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d",date)) AS month,
  fullVisitorId as visitorId,
  visitNumber as visitNumber,
  geoNetwork.country,
  trafficSource.medium,
  trafficSource.source,
  trafficSource.campaign,
  trafficSource.adContent,
  device.browser as browser,
  SUM(IF (hits.eventInfo.eventAction = "Firefox Download",1,0)) as downloads
FROM
  `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
  UNNEST (hits) AS hits
WHERE
  _TABLE_SUFFIX >= '20180101'
  AND _TABLE_SUFFIX <= '20181231'
  AND hits.type = 'EVENT'
  AND hits.eventInfo.eventCategory IS NOT NULL
  AND hits.eventInfo.eventLabel LIKE "Firefox for Desktop%"
GROUP BY
  1,2,3,4,5,6,7,8,9)
GROUP BY 1,2,3,4,5,6) as hitsTable

ON
sessionsTable.month = hitsTable.month
AND sessionsTable.country = hitsTable.country
AND sessionsTable.medium = hitsTable.medium
AND sessionsTable.source = hitsTable.source
AND sessionsTable.campaign = hitsTable.campaign
AND sessionsTable.adContent = hitsTable.adContent

LEFT JOIN
-- Pull Fetch Spend data
(SELECT
  adName,
  SUM(VendorNetSpend) as vendorNetSpend,
  SUM(TrackedClicks) as trackedClicks,
  SAFE_DIVIDE(SUM(VendorNetSpend),SUM(TrackedClicks)) as costPerClick
FROM
  `fetch.fetch_deduped`
GROUP BY 1) fetchSpend

ON
sessionsTable.adContent = fetchSpend.adName)

GROUP BY 1,2,3,4,5,6,11
ORDER BY 1,3,4 DESC )

--- Pull from all data table

SELECT * FROM
(SELECT
campaign,
adContent,
country,
source,
CASE WHEN campaign LIKE '%|US|%' THEN 'United States' ELSE
  CASE WHEN campaign LIKE '%-US-%' THEN 'United States' ELSE
  CASE WHEN campaign LIKE '%-CA-%' THEN 'Canada' ELSE
  CASE WHEN campaign LIKE '%|CA|%' THEN 'Canada' ELSE
  CASE WHEN campaign LIKE '%-DE-%' THEN 'Germany' ELSE
  CASE WHEN campaign LIKE '%|DE|%' THEN 'Germany' ELSE
  CASE WHEN campaign LIKE '%-FR-%' THEN 'France' ELSE
  CASE WHEN campaign LIKE '%|FR|%' THEN 'France' ELSE
  CASE WHEN campaign LIKE '%-UK-%' THEN 'United Kingdom' ELSE
  CASE WHEN campaign LIKE '%|UK|%' THEN 'United Kingdom' ELSE 'other'
  END END END END END END END END END END as targetCountry,
CASE WHEN campaign LIKE '%Brand%' THEN 'Brand' ELSE 'NonBrand' END as semType,
SUM(totalSessions) as totalSessions,
SUM(nonFxSessions) as nonFxSessions,
SUM(downloads) as downloads,
SUM(nonFXDownloads) as nonFXDownloads,
costPerTrackedClick
FROM alldata
WHERE
source IN ('google', 'bing')
AND medium = 'cpc'
GROUP BY 1,2,3,4,5,6,11
ORDER BY semType DESC, totalSessions DESC)
WHERE targetCountry IN ('United States', 'Canada', 'Germany', 'France', 'United Kingdom')