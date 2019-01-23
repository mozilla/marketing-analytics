WITH data AS(
SELECT
metrics.submissionDate,
metrics.funnelOrigin,
metrics.DAU,
metrics.aDAU,
metrics.installs,
metrics.searches,
total.totalDAU,
total.totalaDAU,
total.totalInstalls,
total.totalSearches
FROM(
(
SELECT
  submission_date_s3 AS submissionDate,
  funnelOrigin,
  sum(DAU) as DAU,
  sum(activeDAU) as aDAU,
  sum(installs) as installs,
  sum(searches) as searches
FROM
  `ga-mozilla-org-prod-001.telemetry.corpMetrics`
  GROUP BY 1,2

UNION ALL

SELECT
  submission_date_s3 AS submissionDate,
  'mktgAttrFunnel' as funnelOrigin,
  sum(DAU) as DAU,
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
  ROUND(AVG(DAU) OVER (ORDER BY funnelOrigin, submissionDate ROWS BETWEEN 91 PRECEDING AND CURRENT ROW)) AS DAUMetric,
  aDAU,
  ROUND(AVG(aDAU) OVER (ORDER BY funnelOrigin, submissionDate ROWS BETWEEN 91 PRECEDING AND CURRENT ROW)) AS aDAUMetric,
  totalaDAU,
  SAFE_DIVIDE(ROUND(AVG(aDAU) OVER (ORDER BY funnelOrigin, submissionDate ROWS BETWEEN 91 PRECEDING AND CURRENT ROW)), ROUND(AVG(totalaDAU) OVER (ORDER BY funnelOrigin, submissionDate ROWS BETWEEN 91 PRECEDING AND CURRENT ROW))) AS aDAUMetricPercTotal
FROM data
GROUP BY 1,2,3,5,7