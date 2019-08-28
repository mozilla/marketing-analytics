WITH alldata as(
SELECT
sessionsTable.month,
sessionsTable.country,
sessionsTable.channels,
SUM(sessionsTable.totalSessions) as totalSessions,
SUM(sessionsTable.nonFxSessions) as nonFxSessions,
SUM(hitsTable.downloads) as downloads,
SUM(hitsTable.nonFXDownloads) as nonFXDownloads

FROM(
-- session level metrics --
(SELECT
month,
country,
channels,
SUM(siteSessions) as totalSessions,
SUM(CASE WHEN browser != 'Firefox' THEN siteSessions ELSE 0 END) as nonFxSessions
FROM
(SELECT
  FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d",date)) AS month,
  CASE
    WHEN geoNetwork.country IN ('Canada',  'France',  'Germany',  'United Kingdom',  'United States') THEN geoNetwork.country
    ELSE 'rest of world'
  END AS country,
  CASE
    WHEN trafficSource.medium = 'organic' THEN 'organic'
    ELSE CASE
    WHEN trafficSource.medium IN ('banner',  'cpc',  'display',  'paidsearch',  'ppc',  'social',  'video') THEN 'paid'
    ELSE CASE
    WHEN trafficSource.medium IN ('blog',  'download_button',  'email',  'firefox-browser',  'fx-accounts',  'lp',  'native',  'show-heartbeat',  'snippet',  'static',  'tiles') THEN 'owned'
    ELSE CASE
    WHEN trafficSource.medium = '(none)' AND trafficSource.source = '(direct)' THEN 'direct'
    ELSE CASE
    WHEN trafficSource.medium = 'referral' THEN 'referral'
    ELSE 'other'
  END END
  END END
  END AS channels,
  device.browser AS browser,
  SUM(totals.visits) AS siteSessions
FROM
  `ga-mozilla-org-prod-001.65789850.ga_sessions_*`
WHERE
  _TABLE_SUFFIX >= '20190501'
  AND _TABLE_SUFFIX <= '20190531'
GROUP BY 1,2,3,4)
GROUP BY 1,2,3
) AS sessionsTable

LEFT JOIN

-- hits based metrics --
(SELECT
month,
country,
channels,
SUM(IF(downloads > 0,1,0)) as downloads,
SUM(IF(downloads > 0 AND browser != 'Firefox',1,0)) as nonFXDownloads
FROM
(SELECT
  FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d",date)) AS month,
  fullVisitorId as visitorId,
  visitNumber as visitNumber,
  CASE
    WHEN geoNetwork.country IN ('Canada',  'France',  'Germany',  'United Kingdom',  'United States') THEN geoNetwork.country
    ELSE 'rest of world'
  END AS country,
  CASE
    WHEN trafficSource.medium = 'organic' THEN 'organic'
    ELSE CASE
    WHEN trafficSource.medium IN ('banner',  'cpc',  'display',  'paidsearch',  'ppc',  'social',  'video') THEN 'paid'
    ELSE CASE
    WHEN trafficSource.medium IN ('blog',  'download_button',  'email',  'firefox-browser',  'fx-accounts',  'lp',  'native',  'show-heartbeat',  'snippet',  'static',  'tiles') THEN 'owned'
    ELSE CASE
    WHEN trafficSource.medium = '(none)' AND trafficSource.source = '(direct)' THEN 'direct'
    ELSE CASE
    WHEN trafficSource.medium = 'referral' THEN 'referral'
    ELSE 'other'
  END END
  END END
  END AS channels,
  device.browser as browser,
  SUM(IF (hits.eventInfo.eventAction = "Firefox Download",1,0)) as downloads
FROM
  `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
  UNNEST (hits) AS hits
WHERE
  _TABLE_SUFFIX >= '20190501'
  AND _TABLE_SUFFIX <= '20190531'
  AND hits.type = 'EVENT'
  AND hits.eventInfo.eventCategory IS NOT NULL
  AND hits.eventInfo.eventLabel LIKE "Firefox for Desktop%"
GROUP BY
  1,2,3,4,5,6)
GROUP BY 1,2,3) as hitsTable

ON sessionsTable.month = hitsTable.month AND sessionsTable.country = hitsTable.country AND sessionsTable.channels = hitsTable.channels)

GROUP BY 1,2,3
ORDER BY 1,2,4 DESC)

--- Pull from all data table

SELECT * FROM alldata

UNION ALL

(SELECT
month,
'total' as country,
channels,
SUM(totalSessions) as totalSessions,
SUM(nonFxSessions) as nonFxSessions,
SUM(downloads) as downloads,
SUM(nonFXDownloads) as nonFXDownloads
FROM alldata
GROUP BY 1,2,3
ORDER BY 1,4 DESC)

UNION ALL

(SELECT
month,
'tier 1' as country,
channels,
SUM(totalSessions) as totalSessions,
SUM(nonFxSessions) as nonFxSessions,
SUM(downloads) as downloads,
SUM(nonFXDownloads) as nonFXDownloads
FROM alldata
WHERE country IN ('Canada',  'France',  'Germany',  'United Kingdom',  'United States')
GROUP BY 1,2,3
ORDER BY 1,4 DESC)

UNION ALL

(SELECT
month,
'tier 1-NA' as country,
channels,
SUM(totalSessions) as totalSessions,
SUM(nonFxSessions) as nonFxSessions,
SUM(downloads) as downloads,
SUM(nonFXDownloads) as nonFXDownloads
FROM alldata
WHERE country IN ('Canada',  'United States')
GROUP BY 1,2,3
ORDER BY 1,4 DESC)

UNION ALL

(SELECT
month,
'tier 1-EU' as country,
channels,
SUM(totalSessions) as totalSessions,
SUM(nonFxSessions) as nonFxSessions,
SUM(downloads) as downloads,
SUM(nonFXDownloads) as nonFXDownloads
FROM alldata
WHERE country IN ('France',  'Germany',  'United Kingdom')
GROUP BY 1,2,3
ORDER BY 1,4 DESC)

ORDER BY 1,2,4 DESC