WITH parameters as(
SELECT '20190127' as startDate, '20190223' as endDate, /*'en-ca' as locale,*/ '%/en-CA/firefox/new/%' as landingPage),

sessionsData as(
SELECT
  date AS date,
  fullVisitorId as visitorId,
  visitid as visitId,
  CASE WHEN hits.isEntrance IS TRUE THEN page.pagePath END as landingPage,
  geoNetwork.country as country,
  device.language as locale,
  trafficSource.source as source,
  trafficSource.medium as medium,
  trafficSource.campaign as campaign,
  trafficSource.adContent as content,
  device.browser as browser
FROM
  `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
  UNNEST (hits) as hits
WHERE
  _TABLE_SUFFIX >= (SELECT parameters.startDate from parameters)
  AND _TABLE_SUFFIX <= (SELECT parameters.endDate from parameters)
  AND totals.visits = 1
  /*AND device.language = (SELECT parameters.locale FROM parameters)*/
GROUP BY
  date, visitorId, visitId, landingPage, country, locale, source, medium, campaign, content, browser),

sessionsSummary as(
SELECT
  date,
  landingPage,
  country,
  locale,
  source,
  medium,
  campaign,
  content,
  browser,
  CONCAT(CAST(visitorid AS string),CAST(visitId AS string)) as visitIdentifier
FROM sessionsData
WHERE
  landingPage LIKE (SELECT parameters.landingPage FROM parameters)
GROUP BY
  date, landingPage, country, locale, source, medium, campaign, content, browser, visitIdentifier),

downloadsData as(
SELECT
  date as date,
  CONCAT(CAST(visitorid AS string),CAST(visitId AS string)) as visitIdentifier,
  SUM(IF(downloads > 0,1,0)) as downloads,
  SUM(IF(downloads > 0 AND browser != 'Firefox',1,0)) as nonFXDownloads
FROM (
      SELECT
        date AS date,
        fullVisitorId as visitorId,
        visitid as visitId,
        device.browser as browser,
        SUM(IF (hits.eventInfo.eventAction = "Firefox Download",1,0)) as downloads
      FROM
        `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
        UNNEST (hits) AS hits
      WHERE
        _TABLE_SUFFIX >= (SELECT parameters.startDate from parameters)
        AND _TABLE_SUFFIX <= (SELECT parameters.endDate from parameters)
        AND hits.type = 'EVENT'
        AND hits.eventInfo.eventCategory IS NOT NULL
        AND hits.eventInfo.eventLabel LIKE "Firefox for Desktop%"
      GROUP BY
        date, visitorId, visitId, browser)
GROUP BY date, visitIdentifier
ORDER BY date
),

trafficData as(
SELECT
  sessionsSummary.date,
  sessionsSummary.country,
  sessionsSummary.locale,
  sessionsSummary.source,
  sessionsSummary.medium,
  sessionsSummary.campaign,
  sessionsSummary.content,
  sessionsSummary.browser,
  sessionsSummary.visitIdentifier,
  SUM(downloadsData.downloads) as downloads,
  SUM(downloadsData.nonFXDownloads) as nonFXDownloads
FROM
  sessionsSummary
LEFT JOIN
  downloadsData
ON
  sessionsSummary.visitIdentifier = downloadsData.visitIdentifier
/*WHERE
  medium IN ('organic', 'referral', '(none)')
  AND browser != 'Firefox'*/
GROUP BY
  date, country, locale, source, medium, campaign, content, browser, visitIdentifier
)

SELECT
 EXTRACT (WEEK FROM (PARSE_DATE('%Y%m%d',date))) as weekNum,
 MIN(date) as weekStart,
 MAX(date) as weekEnd,
 COUNT(DISTINCT visitIdentifier) as sessions,
 SUM(downloads) as downloads,
 SUM(nonFXDownloads) as nonFXDownloads
FROM
  trafficData
GROUP BY
  weekNum
ORDER BY
  weekNum DESC,
  sessions DESC