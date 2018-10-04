WITH data AS (
  SELECT
  submission_date_s3 AS submissionDate,
  funnelOrigin AS funnelOrigin,
  sum(DAU) as DAU,
  sum(activeDAU) as aDAU,
  sum(installs) as installs,
  sum(searches) as searches
FROM
  `ga-mozilla-org-prod-001.telemetry.corpMetrics`
GROUP By 1,2
UNION ALL
SELECT
  submission_date_s3 AS submissionDate,
  'total' AS funnelOrigin,
  sum(DAU) as DAU,
  sum(activeDAU) as aDAU,
  sum(installs) as installs,
  sum(searches) as searches
FROM
  `ga-mozilla-org-prod-001.telemetry.corpMetrics`
GROUP By 1,2
ORDER BY 2,1)
  
SELECT 
  submissionDate,
  funnelOrigin,
  DAU,
  aDAU,
  installs,
  searches,
  ROUND(AVG(DAU) OVER (ORDER BY funnelOrigin, submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS DAU28Day,
  ROUND(AVG(aDAU) OVER (ORDER BY funnelOrigin, submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS aDAU28Day,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' THEN installs ELSE 0 END) as mozInstalls,
  SUM(CASE WHEN funnelOrigin = 'darkFunnel' THEN installs ELSE 0 END) as darkFunnelInstalls,
  SUM(CASE WHEN funnelOrigin = 'total' THEN installs ELSE 0 END) as totalInstalls
FROM data
GROUP BY 1,2,3,4,5,6