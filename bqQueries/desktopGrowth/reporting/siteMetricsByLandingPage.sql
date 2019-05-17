WITH parameters as(
SELECT
'20171215' as startDate,
'20171215' as endDate
),

sessionsTable as(SELECT
  date,
  language,
  landingPage,
  pagePathLevel1,
  pageName,
  visitIdentifier,
  country,
  source,
  medium,
  campaign,
  adContent,
  browser,
  SUM(entrances) as sessions,
  SUM(CASE WHEN browser != 'Firefox' THEN entrances ELSE 0 END) as nonFXSessions
FROM (
  SELECT
    date,
    device.language,
    hits.page.pagePath as landingPage,
    hits.page.pagePathLevel1 as pagePathLevel1,
    CONCAT('/',REPLACE(hits.page.pagePathLevel2,'/',''),'/',REPLACE(hits.page.pagePathLevel3,'/',''),'/') as pageName,
    CONCAT(CAST(fullVisitorId AS string),CAST(visitId AS string)) as visitIdentifier,
    geoNetwork.country,
    trafficSource.source,
    trafficSource.medium,
    trafficSource.campaign,
    trafficSource.adContent,
    device.browser,
    CASE WHEN hits.isEntrance IS NOT NULL THEN 1 ELSE 0 END as entrances
  FROM `ga-mozilla-org-prod-001.65789850.ga_sessions_*`, UNNEST (hits) as hits
  WHERE
    _TABLE_SUFFIX >= (SELECT startDate FROM parameters)
    AND _TABLE_SUFFIX <= (SELECT endDate FROM parameters))
GROUP BY date, language, landingPage, pagePathLevel1, pageName, visitIdentifier, country, source, medium, campaign, adContent, browser),

downloadsTable as(
SELECT
  date,
  language,
  visitIdentifier,
  country,
  source,
  medium,
  campaign,
  adContent,
  browser,
  SUM(IF(downloads > 0,1,0)) as downloads,
  SUM(IF(downloads > 0 AND browser != 'Firefox',1,0)) as nonFXDownloads
FROM (SELECT
  date AS date,
  CONCAT(CAST(fullVisitorId AS string),CAST(visitId AS string)) as visitIdentifier,
  device.language,
  geoNetwork.country,
  trafficSource.source,
  trafficSource.medium,
  trafficSource.campaign,
  trafficSource.adContent,
  device.browser as browser,
  SUM(IF (hits.eventInfo.eventAction = "Firefox Download",1,0)) as downloads
FROM
  `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
  UNNEST (hits) AS hits
WHERE
  _TABLE_SUFFIX >= (SELECT startDate FROM parameters)
  AND _TABLE_SUFFIX <= (SELECT endDate FROM parameters)
  AND hits.type = 'EVENT'
  AND hits.eventInfo.eventCategory IS NOT NULL
  AND hits.eventInfo.eventLabel LIKE "Firefox for Desktop%"
GROUP BY
  date,visitIdentifier, language, country, source, medium, campaign, adContent, browser)
GROUP BY date, language, visitIdentifier, country, source, medium, campaign, adContent, browser),

joinedTable as (
SELECT
  sessionsTable.date,
  sessionsTable.visitIdentifier,
  sessionsTable.language,
  sessionsTable.landingPage,
  sessionsTable.pagePathLevel1,
  sessionsTable.pageName,
  sessionsTable.country,
  sessionsTable.source,
  sessionsTable.medium,
  sessionsTable.campaign,
  sessionsTable.adContent,
  sessionsTable.browser,
  sessionsTable.sessions,
  sessionsTable.nonFXSessions,
  downloadsTable.downloads,
  downloadsTable.nonFxDownloads
FROM
  sessionsTable
FULL JOIN
  downloadsTable
ON
  sessionsTable.date = downloadsTable.date
  AND sessionsTable.visitIdentifier = downloadsTable.visitIdentifier
WHERE sessionsTable.sessions != 0)

SELECT
PARSE_DATE('%Y%m%d', date) as date,
language,
landingPage,
pagePathLevel1,
pageName,
country,
source,
medium,
campaign,
adContent,
browser,
SUM(sessions) as sessions,
SUM(nonFXSessions) as nonFXSessions,
SUM(downloads) as downloads,
SUM(nonFXDownloads) as nonFXDownloads
FROM joinedTable
GROUP BY
  date, language, landingPage, pagePathLevel1, pageName, country, source, medium, campaign, adContent, browser