WITH parameters as(
SELECT
  '20190220' as startDate,
  '20190220' as endDate
),

sessionsTable as (
  SELECT
    PARSE_DATE('%Y%m%d', date) as date,
    CASE WHEN country IS NULL THEN '' ELSE country END as country,
    CASE WHEN source IS NULL THEN '' ELSE source END as source,
    CASE WHEN medium IS NULL THEN '' ELSE medium END as medium,
    CASE WHEN campaign IS NULL THEN '' ELSE campaign END as campaign,
    CASE WHEN content IS NULL THEN '' ELSE content END as content,
    SUM(sessions) as sessions,
    SUM(CASE WHEN browser != 'Firefox' THEN sessions ELSE 0 END) as nonFXSessions
    FROM(
      SELECT
        date,
        country,
        source,
        medium,
        campaign,
        content,
        browser,
        COUNT(DISTINCT visitIdentifier) as sessions
      FROM(
          SELECT
            date AS date,
            CONCAT(CAST(fullVisitorId AS string),CAST(visitId AS string)) as visitIdentifier,
            geoNetwork.country as country,
            trafficSource.source as source,
            trafficSource.medium as medium,
            trafficSource.campaign as campaign,
            trafficSource.adcontent as content,
            device.browser as browser
          FROM
            `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
            UNNEST (hits) AS hits
          WHERE
            _TABLE_SUFFIX >= (SELECT parameters.startDate from parameters)
            AND _TABLE_SUFFIX <= (SELECT parameters.startDate from parameters)
            AND totals.visits = 1
          GROUP BY
            date,visitIdentifier, country, source, medium, campaign, content, browser)
      GROUP BY
        date, country, source, medium, campaign, content,browser)
  GROUP BY date, country, source, medium, campaign, content),


downloadsTable as(
SELECT
  PARSE_DATE('%Y%m%d', date) as date,
  CASE WHEN country IS NULL THEN '' ELSE country END as country,
  CASE WHEN source IS NULL THEN '' ELSE source END as source,
  CASE WHEN medium IS NULL THEN '' ELSE medium END as medium,
  CASE WHEN campaign IS NULL THEN '' ELSE campaign END as campaign,
  CASE WHEN content IS NULL THEN '' ELSE content END as content,
  SUM(IF(downloads > 0,1,0)) as downloads,
  SUM(IF(downloads > 0 AND browser != 'Firefox',1,0)) as nonFXDownloads
FROM (SELECT
  date AS date,
  fullVisitorId as visitorId,
  visitNumber as visitNumber,
  geoNetwork.country as country,
  trafficSource.source as source,
  trafficSource.medium as medium,
  trafficSource.campaign as campaign,
  trafficSource.adcontent as content,
  device.browser as browser,
  SUM(IF (hits.eventInfo.eventAction = "Firefox Download",1,0)) as downloads
FROM
  `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
  UNNEST (hits) AS hits
WHERE
  _TABLE_SUFFIX >= (SELECT parameters.startDate from parameters)
  AND _TABLE_SUFFIX <= (SELECT parameters.startDate from parameters)
  AND hits.type = 'EVENT'
  AND hits.eventInfo.eventCategory IS NOT NULL
  AND hits.eventInfo.eventLabel LIKE "Firefox for Desktop%"
GROUP BY
  date,visitorID, visitNumber, country, source, medium, campaign, content, browser)
GROUP BY date, country, source, medium, campaign, content
),

siteData as (
SELECT
  sessionsTable.date,
  sessionsTable.country,
  sessionsTable.source,
  sessionsTable.medium,
  sessionsTable.campaign,
  sessionsTable.content,
  SUM(sessionsTable.sessions) as sessions,
  SUM(sessionsTable.nonFXSessions) as nonFXSessions,
  SUM(downloadsTable.downloads) as downloads,
  SUM(downloadsTable.nonFXDownloads) as nonFXDownloads
FROM
  sessionsTable
LEFT JOIN
  downloadsTable
ON
  sessionsTable.date = downloadsTable.date
  AND sessionsTable.country = downloadsTable.country
  AND sessionsTable.source = downloadsTable.source
  AND sessionsTable.medium = downloadsTable.medium
  AND sessionsTable.campaign = downloadsTable.campaign
  AND sessionsTable.content = downloadsTable.content
GROUP BY
  date, country, source, medium, campaign, content
),

cleanedSiteData as (
SELECT
  siteData.*,
  standardizedCountryList.standardizedCountry as countryCleaned
FROM
  siteData
LEFT JOIN
  `lookupTables.standardizedCountryList` as standardizedCountryList
ON
  siteData.country = standardizedCountryList.rawCountry
)

SELECT * from cleanedSiteData