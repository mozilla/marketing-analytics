WITH impressionData AS(
SELECT
    visitData.date,
    visitData.snippetID,
    visitData.country,
    visitData.eventCategory,
    -- Get statistics for top 3 events. All other = other
    CASE WHEN eventCategory = 'impression' THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS impression,
    CASE WHEN eventCategory = 'snippet-blocked' THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS snippetBlocked,
    CASE WHEN eventCategory = 'click' OR eventCategory = 'button-click' THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS clicks,
    CASE WHEN eventCategory NOT IN('impression','snippet-blocked', 'click','button-click') THEN COUNT(DISTINCT(fullVisitorId)) ELSE 0 END AS other
FROM (
    SELECT
    date,
    geoNetwork.country,
    fullVisitorId,
    eventInfo.eventAction AS snippetID,
    eventInfo.eventCategory
    FROM
    `ga-mozilla-org-prod-001.125230768.ga_sessions_*`,
    UNNEST (hits) AS hits
    WHERE
    _TABLE_SUFFIX = '20190108'
    GROUP BY 1,2,3,4,5) AS visitData
GROUP BY
    1,2,3,4
ORDER BY 4 DESC),

-- Pull data from addons.mozilla.org

addonsData AS(SELECT
    date AS date,
    trafficSource.keyword AS snippetID,
    geoNetwork.country AS country,
    SUM(totals.visits) AS sessions,
    SUM((SELECT SUM(DISTINCT IF (REGEXP_CONTAINS(hits.eventInfo.eventCategory, '^AMO (?:Addon|Theme|Addon / Theme) Installs$') AND hits.eventInfo.eventAction = 'addon',1,0)) FROM UNNEST(hits) hits)) AS sessionsInstallingAddons,
    SUM((SELECT SUM(IF (REGEXP_CONTAINS(hits.eventInfo.eventCategory, '^AMO (?:Addon|Theme|Addon / Theme) Installs$') AND hits.eventInfo.eventAction = 'addon',1,0)) FROM UNNEST(hits) hits)) AS totalAddonsInstalled,
    SUM((SELECT SUM(DISTINCT IF (REGEXP_CONTAINS(hits.eventInfo.eventCategory, '^AMO (?:Addon|Theme|Addon / Theme) Installs$') AND hits.eventInfo.eventAction = 'theme',1,0)) FROM UNNEST(hits) hits)) AS sessionsInstallingThemes,
    SUM((SELECT SUM(IF (REGEXP_CONTAINS(hits.eventInfo.eventCategory, '^AMO (?:Addon|Theme|Addon / Theme) Installs$') AND hits.eventInfo.eventAction = 'theme',1,0)) FROM UNNEST(hits) hits)) AS totalThemesInstalled
FROM `ga-mozilla-org-prod-001.67693596.ga_sessions_*`
WHERE
_TABLE_SUFFIX = '20190108'
AND trafficSource.medium = 'snippet'
GROUP BY 1,2,3
ORDER BY 2 ASC, 4 DESC),

-- Pull data from mozilla.org
mozorgData AS(
SELECT
date as date,
trafficSource.keyword as snippetID,
geoNetwork.country as country,
SUM(totals.visits) AS sessions
FROM
`ga-mozilla-org-prod-001.65789850.ga_sessions_*`
WHERE
_TABLE_SUFFIX = '20190108'
AND trafficSource.medium = 'snippet'
GROUP By 1,2,3
ORDER BY 4 DESC
),

-- Pull data from blog.mozilla.org
blogData AS(
SELECT
  date as date,
  trafficSource.keyword as snippetID,
  geoNetwork.country as country,
  SUM(totals.visits) AS sessions
FROM
  `ga-mozilla-org-prod-001.66602784.ga_sessions_*`
WHERE
  _TABLE_SUFFIX = '20190108'
  AND trafficSource.medium = 'snippet'
GROUP By 1,2,3
ORDER BY 4 DESC
),

-- Pull data from testpilot.firefox.com
testPilotData AS(
SELECT
  date as date,
  trafficSource.keyword as snippetID,
  geoNetwork.country as country,
  SUM(totals.visits) AS sessions
FROM
  `ga-mozilla-org-prod-001.106368739.ga_sessions_*`
WHERE
  _TABLE_SUFFIX = '20190108'
  AND trafficSource.medium = 'snippet'
GROUP By 1,2,3
ORDER BY 4 DESC
),

-- Pull data from developer.mozilla.org
developerData AS(
SELECT
  date as date,
  trafficSource.keyword as snippetID,
  geoNetwork.country as country,
  SUM(totals.visits) AS sessions
FROM
  `ga-mozilla-org-prod-001.66726481.ga_sessions_*`
WHERE
  _TABLE_SUFFIX = '20190108'
  AND trafficSource.medium = 'snippet'
GROUP By 1,2,3
ORDER BY 4 DESC
),

-- Pull data from support.mozilla.org
sumoData AS(
SELECT
  date as date,
  trafficSource.keyword as snippetID,
  geoNetwork.country as country,
  SUM(totals.visits) AS sessions
FROM
  `ga-mozilla-org-prod-001.65912487.ga_sessions_*`
WHERE
  _TABLE_SUFFIX = '20190108'
  AND trafficSource.medium = 'snippet'
GROUP By 1,2,3
ORDER BY 4 DESC
),

-- Pull data from hacks.mozilla.org
hacksData AS(
SELECT
  date as date,
  trafficSource.keyword as snippetID,
  geoNetwork.country as country,
  SUM(totals.visits) AS sessions
FROM
  `ga-mozilla-org-prod-001.65887927.ga_sessions_*`
WHERE
  _TABLE_SUFFIX = '20190108'
  AND trafficSource.medium = 'snippet'
GROUP By 1,2,3
ORDER BY 4 DESC
),

-- Pull data from donate.mozilla.org
donateData AS(
SELECT
    date AS date,
    trafficSource.keyword AS snippetID,
    geoNetwork.country AS country,
    SUM(totals.visits) AS sessions,
    SUM((SELECT SUM(DISTINCT IF(REGEXP_CONTAINS(page.pagePath, '/thank-you/'),1,0)) FROM UNNEST(hits) )) AS donations
  FROM
    `ga-mozilla-org-prod-001.105783219.ga_sessions_*`
  WHERE
  _TABLE_SUFFIX = '20190108'
  AND trafficSource.medium = 'snippet'
  GROUP BY 1,2,3
  ORDER BY 2 ASC,4 DESC
),

aggregates AS(
-- Aggregate by date, snippetID, country and site
SELECT
  PARSE_DATE('%Y%m%d', impressions.date) as date,
  impressions.snippetID,
  impressions.country,
  'snippets tracking' as site,
  SUM(impressions.impression)*1000 AS impression,
  SUM(impressions.snippetBlocked)*1000 AS snippetBlocked,
  SUM(impressions.clicks)*1000 AS clicks,
  SUM(impressions.other)*1000 as otherSnippetInteractions,
  NULL as sessions,
  NULL as addonInstallsTotal,
  NULL as addonInstallsGoalComp,
  NULL as themeInstallsTotal,
  NULL as themeInstallsGoalComp,
  NULL as donations
FROM impressionData as impressions
GROUP By 1,2,3,4

-- Join addons data
UNION ALL
SELECT
  PARSE_DATE('%Y%m%d', addonsData.date) as date,
  addonsData.snippetID,
  addonsData.country,
  'addons.mozilla.org' as site,
  NULL as impression,
  NULL as snippetBlocked,
  NULL as clicks,
  NULL as otherSnippetInteractions,
  SUM(addonsData.sessions) as sessions,
  SUM(addonsData.totalAddonsInstalled) as addonInstallsTotal,
  SUM(addonsData.sessionsInstallingAddons) as addonInstallsGoalComp,
  SUM(addonsData.totalThemesInstalled) as themeInstallsTotal,
  SUM(addonsData.sessionsInstallingThemes) as themeInstallsGoalComp,
  NULL as donations
FROM addonsData
GROUP BY 1,2,3,4

-- Join mozilla.org data
UNION ALL
SELECT
  PARSE_DATE('%Y%m%d', mozorgData.date) as date,
  mozorgData.snippetID,
  mozorgData.country,
  'mozilla.org' as site,
  NULL as impression,
  NULL as snippetBlocked,
  NULL as clicks,
  NULL as otherSnippetInteractions,
  SUM(mozorgData.sessions) as sessions,
  NULL as addonInstallsTotal,
  NULL as addonInstallsGoalComp,
  NULL as themeInstallsTotal,
  NULL as themeInstallsGoalComp,
  NULL as donations
FROM mozorgData
GROUP BY 1,2,3,4

-- Join blog.mozilla.org data
UNION ALL
SELECT
  PARSE_DATE('%Y%m%d', blogData.date) as date,
  blogData.snippetID,
  blogData.country,
  'blog.mozilla.org' as site,
  NULL as impression,
  NULL as snippetBlocked,
  NULL as clicks,
  NULL as otherSnippetInteractions,
  SUM(blogData.sessions) as sessions,
  NULL as addonInstallsTotal,
  NULL as addonInstallsGoalComp,
  NULL as themeInstallsTotal,
  NULL as themeInstallsGoalComp,
  NULL as donations
FROM blogData
GROUP BY 1,2,3,4

-- Join testpilot.firefox.com data
UNION ALL
SELECT
  PARSE_DATE('%Y%m%d', testPilotData.date) as date,
  testPilotData.snippetID,
  testPilotData.country,
  'testpilot.firefox.com' as site,
  NULL as impression,
  NULL as snippetBlocked,
  NULL as clicks,
  NULL as otherSnippetInteractions,
  SUM(testPilotData.sessions) as sessions,
  NULL as addonInstallsTotal,
  NULL as addonInstallsGoalComp,
  NULL as themeInstallsTotal,
  NULL as themeInstallsGoalComp,
  NULL as donations
FROM testPilotData
GROUP BY 1,2,3,4

-- Join developer.mozilla.org data
UNION ALL
SELECT
  PARSE_DATE('%Y%m%d', developerData.date) as date,
  developerData.snippetID,
  developerData.country,
  'developer.mozilla.org' as site,
  NULL as impression,
  NULL as snippetBlocked,
  NULL as clicks,
  NULL as otherSnippetInteractions,
  SUM(developerData.sessions) as sessions,
  NULL as addonInstallsTotal,
  NULL as addonInstallsGoalComp,
  NULL as themeInstallsTotal,
  NULL as themeInstallsGoalComp,
  NULL as donations
FROM developerData
GROUP BY 1,2,3,4

-- Join support.mozilla.org data
UNION ALL
SELECT
  PARSE_DATE('%Y%m%d', sumoData.date) as date,
  sumoData.snippetID,
  sumoData.country,
  'support.mozilla.org' as site,
  NULL as impression,
  NULL as snippetBlocked,
  NULL as clicks,
  NULL as otherSnippetInteractions,
  SUM(sumoData.sessions) as sessions,
  NULL as addonInstallsTotal,
  NULL as addonInstallsGoalComp,
  NULL as themeInstallsTotal,
  NULL as themeInstallsGoalComp,
  NULL as donations
FROM sumoData
GROUP BY 1,2,3,4

-- Join hacks.mozilla.org data
UNION ALL
SELECT
  PARSE_DATE('%Y%m%d', hacksData.date) as date,
  hacksData.snippetID,
  hacksData.country,
  'support.mozilla.org' as site,
  NULL as impression,
  NULL as snippetBlocked,
  NULL as clicks,
  NULL as otherSnippetInteractions,
  SUM(hacksData.sessions) as sessions,
  NULL as addonInstallsTotal,
  NULL as addonInstallsGoalComp,
  NULL as themeInstallsTotal,
  NULL as themeInstallsGoalComp,
  NULL as donations
FROM hacksData
GROUP BY 1,2,3,4

-- Join donate.mozilla.org data
UNION ALL
SELECT
  PARSE_DATE('%Y%m%d', donateData.date) as date,
  donateData.snippetID,
  donateData.country,
  'donate.mozilla.org' as site,
  NULL as impression,
  NULL as snippetBlocked,
  NULL as clicks,
  NULL as otherSnippetInteractions,
  SUM(donateData.sessions) as sessions,
  NULL as addonInstallsTotal,
  NULL as addonInstallsGoalComp,
  NULL as themeInstallsTotal,
  NULL as themeInstallsGoalComp,
  SUM(donateData.donations) as donations
FROM donateData
GROUP BY 1,2,3,4

-- Join telemetry tracking data
UNION ALL
SELECT
  sendDate,
  messageID,
  countryCode,
  'telemetry tracking' as site,
  SUM(impressions) as impression,
  SUM(blocks) as snippetBlocked,
  SUM(clicks) as clicks,
  NULL as other,
  NULL as sessions,
  NULL as addonInstallsTotal,
  NULL as addonInstallsGoalComp,
  NULL as themeInstallsTotal,
  NULL as themeInstallsGoalComp,
  NULL as donations
FROM `ga-mozilla-org-prod-001.snippets.snippets_telemetry_tracking_20190108`
GROUP BY 1,2,3,4
ORDER BY 5 DESC),

metaData as (
SELECT
  *
FROM `ga-mozilla-org-prod-001.snippets.snippets_metadata`)

SELECT
  aggregates.*,
  metaData.name,
  metaData.campaign,
  metaData.category,
  metaData.url,
  metaData.body
FROM
  aggregates
LEFT JOIN
  metaData
ON
  aggregates.snippetID = metaData.ID