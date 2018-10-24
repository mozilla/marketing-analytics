WITH addonsData AS(
SELECT
sessionData.date,
sessionData.snippetID,
sessionData.country,
sessionData.sessions,
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
  count(visitId) as visits,
  SUM(totals.visits) AS sessions
FROM
  `ga-mozilla-org-prod-001.67693596.ga_sessions_20181013`
WHERE
  trafficSource.medium = 'snippet'
GROUP By 1,2,3
ORDER BY 4 DESC) as sessionData

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

ON sessionData.date = hitsData.date
AND sessionData.snippetID = hitsData.snippetID
AND sessionData.country = hitsData.country))

SELECT * FROM addonsData