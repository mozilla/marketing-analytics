WITH sessionData AS (
  SELECT
    date AS date,
    trafficSource.keyword AS snippetID,
    geoNetwork.country AS country,
    SUM(totals.visits) AS sessions,
    SUM((
      SELECT
        SUM(DISTINCT IF (REGEXP_CONTAINS(hits.eventInfo.eventCategory, '^AMO (?:Addon|Theme|Addon / Theme) Installs$') AND hits.eventInfo.eventAction = 'addon',1,0))
      FROM
        UNNEST(hits) hits)) AS sessonsInstallingAddons,
    SUM((
      SELECT
        SUM(IF (REGEXP_CONTAINS(hits.eventInfo.eventCategory, '^AMO (?:Addon|Theme|Addon / Theme) Installs$') AND hits.eventInfo.eventAction = 'addon',1,0))
      FROM
        UNNEST(hits) hits)) AS totalAddonsInstalled,
   SUM((
      SELECT
        SUM(DISTINCT IF (REGEXP_CONTAINS(hits.eventInfo.eventCategory, '^AMO (?:Addon|Theme|Addon / Theme) Installs$') AND hits.eventInfo.eventAction = 'theme',1,0))
      FROM
        UNNEST(hits) hits)) AS sessonsInstallingThemes,
   SUM((
      SELECT
        SUM(IF (REGEXP_CONTAINS(hits.eventInfo.eventCategory, '^AMO (?:Addon|Theme|Addon / Theme) Installs$') AND hits.eventInfo.eventAction = 'theme',1,0))
      FROM
        UNNEST(hits) hits)) AS totalThemesInstalled
  FROM
    `ga-mozilla-org-prod-001.67693596.ga_sessions_20181013`
  WHERE
    trafficSource.medium = 'snippet'
  GROUP BY
    1,2,3
  ORDER BY
    2 ASC, 4 DESC)

SELECT * FROM sessionData
