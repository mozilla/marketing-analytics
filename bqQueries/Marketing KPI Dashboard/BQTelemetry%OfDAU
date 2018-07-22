-- Calculate % of DAU that is aDAU

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
  'DAU' as dauType,
  ROUND(AVG(DAU) OVER (ORDER BY funnelOrigin, submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS DAUMetric
FROM data
GROUP BY 1,2,3,4
UNION ALL
SELECT
  submissionDate,
  funnelOrigin,
  aDAU as dauType,
  'aDAU',
  ROUND(AVG(aDAU) OVER (ORDER BY funnelOrigin, submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS DAUMetric
FROM data
GROUP BY 1,2,3,4