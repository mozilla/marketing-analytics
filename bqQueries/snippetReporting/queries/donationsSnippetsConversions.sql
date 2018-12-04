WITH sessionData AS (
  SELECT
    date AS date,
    trafficSource.keyword AS snippetID,
    geoNetwork.country AS country,
    SUM(totals.visits) AS sessions,
    SUM((SELECT SUM(DISTINCT IF(REGEXP_CONTAINS(page.pagePath, '/thank-you/'),1,0)) FROM UNNEST(hits) )) AS donations
  FROM
    `ga-mozilla-org-prod-001.105783219.ga_sessions_20180212`
  WHERE
  trafficSource.medium = 'snippet'
  GROUP BY 1,2,3
  ORDER BY 2 ASC,4 DESC)

SELECT sum(sessions), sum(donations) FROM sessionData
