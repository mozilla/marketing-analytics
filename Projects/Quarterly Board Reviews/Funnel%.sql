

WITH data AS(
SELECT
metrics.submissionDate,
metrics.funnelOrigin,
metrics.DAU,
metrics.aDAUCalculated,
metrics.aDAU,
metrics.installs,
metrics.searches,
total.totalDAU,
total.totalaDAUCalculated,
total.totalaDAU,
total.totalInstalls,
total.totalSearches
FROM(
(
SELECT
  submission_date_s3 AS submissionDate,
  funnelOrigin,
  sum(DAU) as DAU,
  Sum(DAU)*0.75 as aDAUCalculated,
  sum(activeDAU) as aDAU,
  sum(installs) as installs,
  sum(searches) as searches
FROM
  `ga-mozilla-org-prod-001.telemetry.corpMetrics`
  GROUP BY 1,2

UNION ALL

SELECT
  submission_date_s3 AS submissionDate,
  'advAttrFunnel' as funnelOrigin,
  sum(DAU) as DAU,
  sum(DAU)*0.75 as aDAUCalculated,
  sum(activeDAU) as aDAU,
  sum(installs) as installs,
  sum(searches) as searches
FROM
  `ga-mozilla-org-prod-001.telemetry.corpMetrics`
  WHERE funnelOrigin = 'mozFunnel' AND mediumCleaned != 'organic' AND sourceCleaned != '(direct)'
  GROUP BY 1,2 ) as metrics


LEFT JOIN

(SELECT
  submission_date_s3 AS submissionDate,
  sum(DAU) as totalDAU,
  sum(DAU)*0.75 as totalaDAUCalculated,
  sum(activeDAU) as totalaDAU,
  sum(installs) as totalInstalls,
  sum(searches) as totalSearches
  FROM
  `ga-mozilla-org-prod-001.telemetry.corpMetrics`
  GROUP By 1) as total

ON metrics.submissionDate = total.submissionDate))

SELECT
  submissionDate,
  funnelOrigin,
  DAU,
  ROUND(AVG(DAU) OVER (ORDER BY funnelOrigin, submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS DAU28Day,
  aDAUCalculated,
  ROUND(AVG(aDAUCalculated) OVER (ORDER BY funnelOrigin, submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS aDAUCalculated28Day,
  aDAU,
  ROUND(AVG(aDAU) OVER (ORDER BY funnelOrigin, submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS aDAU28Day,
  totalaDAU,
  SAFE_DIVIDE(ROUND(AVG(aDAU) OVER (ORDER BY funnelOrigin, submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)), ROUND(AVG(totalaDAU) OVER (ORDER BY funnelOrigin, submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW))) AS aDAUMetricPercTotal
FROM data
GROUP BY 1,2,3,5,7,9