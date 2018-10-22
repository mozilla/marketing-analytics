-- Query used to visualize charts in aDAU detail report showing breakdown of aDAU by mktAttr, mozfunnel and darkFunnel

WITH data AS (
-- converts total to darkFunnel-total for more accurate area map chart
  SELECT
  submission_date_s3 AS submissionDate,
  'darkFunnel-total' AS funnelOrigin,
  sum(DAU) as DAU,
  sum(activeDAU) as aDAU,
  sum(installs) as installs,
  sum(searches) as searches
FROM
  `ga-mozilla-org-prod-001.telemetry.corpMetrics`
GROUP By 1,2

UNION ALL
-- Select metrics by mktg Attr funnel and dark funnel
  SELECT
  submission_date_s3 AS submissionDate,
  CASE WHEN funnelOrigin = 'mozFunnel' THEN 'mktgAttrFunnel' ELSE funnelOrigin END AS funnelOrigin,
  sum(DAU) as DAU,
  sum(activeDAU) as aDAU,
  sum(installs) as installs,
  sum(searches) as searches
FROM
  `ga-mozilla-org-prod-001.telemetry.corpMetrics`
GROUP By 1,2

UNION ALL
-- select metrics and create a funnel that is total = moz+dark funnel
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

UNION ALL
-- Select metrics and create a funnel for marketing attributable
SELECT
  submission_date_s3 AS submissionDate,
  'mktgAdvertisingFunnel' AS funnelOrigin,
  sum(DAU) as DAU,
  sum(activeDAU) as aDAU,
  sum(installs) as installs,
  sum(searches) as searches
FROM
  `ga-mozilla-org-prod-001.telemetry.corpMetrics`
WHERE funnelOrigin = 'mozFunnel'
AND mediumCleaned != 'organic'
AND sourceCleaned != '(direct)'
GROUP By 1,2
ORDER BY 2,1)

SELECT
  submissionDate,
  funnelOrigin,
  DAU,
  ROUND(AVG(DAU) OVER (ORDER BY funnelOrigin, submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS DAUMetric,
  aDAU,
  ROUND(AVG(aDAU) OVER (ORDER BY funnelOrigin, submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS aDAUMetric
FROM data
GROUP BY 1,2,3,5