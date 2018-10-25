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
    WHERE _TABLE_SUFFIX = '20181013'
    GROUP BY 1,2,3,4,5) AS visitData
  GROUP BY
    1,2,3,4
  ORDER BY 4 DESC),

-- Pull data from addons.mozilla.org

addonsData AS(
SELECT
addonSessionData.date,
addonSessionData.snippetID,
addonSessionData.country,
'addons.mozilla.org' as site,
addonSessionData.sessions,
hitsData.addonInstallsTotal,
hitsData.addonInstallsGoalComp,
hitsData.themeInstallsTotal,
hitsData.themeInstallsGoalComp
FROM(
-- get session level metrics --
(SELECT
  date as date,
  trafficSource.keyword as snippetID,
  geoNetwork.country as country,
  SUM(totals.visits) AS sessions
FROM
  `ga-mozilla-org-prod-001.67693596.ga_sessions_*`
WHERE
  _TABLE_SUFFIX = '20181013'
  AND trafficSource.medium = 'snippet'
GROUP By 1,2,3
ORDER BY 4 DESC) as addonSessionData

LEFT JOIN
-- get hits level data --
(WITH addonsHitsData AS(
  SELECT
    date AS date,
    visitId AS visitId,
    snippetID as snippetID,
    country as country,
    SUM(addonInstalls) AS totalAddonInstalls,
    SUM(themeInstalls) AS totalThemeInstalls
  FROM (
    SELECT
      date AS date,
      trafficSource.keyword AS snippetID,
      visitId,
      geoNetwork.country AS country,
      hits.eventInfo.eventCategory AS eventCategory,
      hits.eventInfo.eventAction AS eventAction,
      hits.eventInfo.eventLabel AS eventLabel,
      hits.eventInfo.eventValue AS eventValue,
      IF(SUM(CASE WHEN REGEXP_CONTAINS(hits.eventInfo.eventCategory, '^AMO (?:Addon|Theme|Addon / Theme) Installs$') AND hits.eventInfo.eventAction = 'addon' THEN 1 ELSE 0 END)>0,1,0) AS addonInstalls,
      IF(SUM(CASE WHEN REGEXP_CONTAINS(hits.eventInfo.eventCategory, '^AMO (?:Addon|Theme|Addon / Theme) Installs$') AND hits.eventInfo.eventAction = 'theme' THEN 1 ELSE 0 END)>0,1,0) AS themeInstalls
    FROM `ga-mozilla-org-prod-001.67693596.ga_sessions_20181013`, UNNEST (hits) AS hits
    WHERE trafficSource.medium = 'snippet'
    GROUP BY 1,2,3,4,5,6,7,8)
  GROUP BY 1,2,3,4)
SELECT
  date,
  snippetID,
  country,
  SUM(totalAddonInstalls) AS addonInstallsTotal,
  SUM(CASE WHEN totalAddonInstalls > 0 THEN 1 ELSE 0 END) AS addonInstallsGoalComp,
  SUM(totalThemeInstalls) AS themeInstallsTotal,
  SUM(CASE WHEN totalThemeInstalls > 0 THEN 1 ELSE 0 END) AS themeInstallsGoalComp
FROM addonsHitsData
GROUP BY 1,2,3
) as hitsData

ON addonSessionData.date = hitsData.date
AND addonSessionData.snippetID = hitsData.snippetID
AND addonSessionData.country = hitsData.country)),

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
  _TABLE_SUFFIX = '20181013'
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
  _TABLE_SUFFIX = '20181013'
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
  _TABLE_SUFFIX = '20181013'
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
  _TABLE_SUFFIX = '20181013'
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
  _TABLE_SUFFIX = '20181013'
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
  _TABLE_SUFFIX = '20181013'
  AND trafficSource.medium = 'snippet'
GROUP By 1,2,3
ORDER BY 4 DESC
)

-- Aggregate by date, snippetID, country and site
SELECT
  impressions.date,
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
  NULL as themeInstallsGoalComp
FROM impressionData as impressions
GROUP By 1,2,3,4

-- Join addons data
UNION ALL
SELECT
  addonsData.date,
  addonsData.snippetID,
  addonsData.country,
  'addons.mozilla.org' as site,
  NULL as impression,
  NULL as snippetBlocked,
  NULL as clicks,
  NULL as otherSnippetInteractions,
  SUM(addonsData.sessions) as sessions,
  SUM(addonsData.addonInstallsTotal) as addonInstallsTotal,
  SUM(addonsData.addonInstallsGoalComp) as addonInstallsGoalComp,
  SUM(addonsData.themeInstallsTotal) as themeInstallsTotal,
  SUM(addonsData.themeInstallsGoalComp) as themeInstallsGoalComp
FROM addonsData
GROUP BY 1,2,3,4

-- Join mozilla.org data
UNION ALL
SELECT
  mozorgData.date,
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
  NULL as themeInstallsGoalComp
FROM mozorgData
GROUP BY 1,2,3,4

-- Join blog.mozilla.org data
UNION ALL
SELECT
  blogData.date,
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
  NULL as themeInstallsGoalComp
FROM blogData
GROUP BY 1,2,3,4

-- Join testpilot.firefox.com data
UNION ALL
SELECT
  testPilotData.date,
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
  NULL as themeInstallsGoalComp
FROM testPilotData
GROUP BY 1,2,3,4

-- Join developer.mozilla.org data
UNION ALL
SELECT
  developerData.date,
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
  NULL as themeInstallsGoalComp
FROM developerData
GROUP BY 1,2,3,4

-- Join support.mozilla.org data
UNION ALL
SELECT
  sumoData.date,
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
  NULL as themeInstallsGoalComp
FROM sumoData
GROUP BY 1,2,3,4

-- Join hacks.mozilla.org data
UNION ALL
SELECT
  hacksData.date,
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
  NULL as themeInstallsGoalComp
FROM hacksData
GROUP BY 1,2,3,4

ORDER BY 4 DESC