WITH parameters as(
SELECT
'20190328' as startDate,
'20190328' as endDate
),

-- Calculate sessions
sessionsTable as(
SELECT
  date,
  visitIdentifier,
  DLExperimentName,
  DLExperimentVariant,
  country,
  language,
  source,
  medium,
  campaign,
  content,
  browser,
  SUM(entrances) as sessions,
  SUM(CASE WHEN browser != 'Firefox' THEN entrances ELSE 0 END) as nonFXSessions
FROM(
  SELECT
    date AS date,
    CONCAT(CAST(fullVisitorId AS string),CAST(visitId AS string)) as visitIdentifier,
    (SELECT value FROM UNNEST(hits.customDimensions) as x WHERE x.index = 69) as DLExperimentName,
    (SELECT value FROM UNNEST(hits.customDimensions) as x WHERE x.index = 70) as DLExperimentVariant,
    geoNetwork.country,
    device.language,
    trafficSource.source as source,
    trafficSource.medium as medium,
    trafficSource.campaign as campaign,
    trafficSource.adcontent as content,
    device.browser as browser,
    CASE WHEN hits.isEntrance IS NOT NULL THEN 1 ELSE 0 END as entrances
  FROM
    `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
    UNNEST (hits) AS hits
  WHERE
    _TABLE_SUFFIX >= (SELECT startDate from parameters)
    AND _TABLE_SUFFIX <= (SELECT endDate from parameters)
  GROUP BY
    date,visitIdentifier, DLExperimentName, DLExperimentVariant, country, language, source, medium, campaign, content, browser, entrances)
GROUP BY
  date, visitIdentifier, DLExperimentName, DLExperimentVariant, country, language, source, medium, campaign, content, browser),

-- Calculate downloads
downloadsTable as(
SELECT
  date,
  language,
  visitIdentifier,
  country,
  source,
  medium,
  campaign,
  content,
  browser,
  SUM(IF(downloads > 0,1,0)) as downloads,
  SUM(IF(downloads > 0 AND browser != 'Firefox',1,0)) as nonFXDownloads
FROM (
  SELECT
    date AS date,
    CONCAT(CAST(fullVisitorId AS string),CAST(visitId AS string)) as visitIdentifier,
    geoNetwork.country,
    device.language,
    trafficSource.source as source,
    trafficSource.medium as medium,
    trafficSource.campaign as campaign,
    trafficSource.adcontent as content,
    device.browser as browser,
    SUM(IF(hits.eventInfo.eventAction = "Firefox Download",1,0)) as downloads
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
    date,visitIdentifier, country, language, source, medium, campaign, content, browser)
GROUP BY date, language, visitIdentifier, country, source, medium, campaign, content, browser),

-- Join the two tables together
joinedTable as (
SELECT
  sessionsTable.date,
  sessionsTable.visitIdentifier,
  sessionsTable.DLExperimentName,
  sessionsTable.DLExperimentVariant,
  sessionsTable.country,
  sessionsTable.language,
  sessionsTable.source,
  sessionsTable.medium,
  sessionsTable.campaign,
  sessionsTable.content,
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
  date,
  DLExperimentName,
  DLExperimentVariant,
  SUM(sessions) as sessions,
  SUM(nonFXSessions) as nonFXSessions,
  SUM(downloads) as downloads,
  SUM(nonFXDownloads) as nonFXDownloads
from joinedTable
GROUP BY
  date, DLExperimentName, DLExperimentVariant
