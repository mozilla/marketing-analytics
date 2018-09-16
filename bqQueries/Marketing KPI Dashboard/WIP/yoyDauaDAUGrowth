-- Query used to map out year over year aDAU trend
WITH data AS(
SELECT
currentYear.submissionDate,
currentYear.DAU,
currentYear.DAUMetric,
currentYear.aDAU,
currentYear.aDAUMetric,
IFNULL(priorYear.DAUMetric,0) as pyDAUMetric,
IFNULL(priorYear.aDAUMetric,0) as pyaDAUMetric,
ROUND(IFNULL(SAFE_DIVIDE(currentYear.DAUMetric, priorYear.DAUMetric)-1,0),2) as dauMetricYOYVar,
ROUND(IFNULL(SAFE_DIVIDE(currentYear.aDAUMetric, priorYear.aDAUMetric)-1,0),2) as aDauMetricYOYVar
FROM (
(SELECT
submissionDate as submissionDate,
DAU,
ROUND(AVG(DAU) OVER (ORDER BY submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS DAUMetric,
aDAU,
ROUND(AVG(aDAU) OVER (ORDER BY submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS aDAUMetric
FROM (
  SELECT
    submission_date_s3 AS submissionDate,
    sum(DAU) as DAU,
    sum(activeDAU) as aDAU,
    sum(installs) as installs,
    sum(searches) as searches
  FROM
    `ga-mozilla-org-prod-001.telemetry.corpMetrics`
  GROUP By 1)
GROUP BY 1,2,4) as currentYear

LEFT JOIN
(SELECT
submissionDate as submissionDate,
FORMAT_DATE('%Y%m%d', DATE_ADD(PARSE_DATE('%Y%m%d', submissionDate), INTERVAL 1 YEAR)) as PYDate,
DAU,
ROUND(AVG(DAU) OVER (ORDER BY submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS DAUMetric,
aDAU,
ROUND(AVG(aDAU) OVER (ORDER BY submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS aDAUMetric
FROM (
  SELECT
    submission_date_s3 AS submissionDate,
    sum(DAU) as DAU,
    sum(activeDAU) as aDAU,
    sum(installs) as installs,
    sum(searches) as searches
  FROM
    `ga-mozilla-org-prod-001.telemetry.corpMetrics`
  GROUP By 1)
GROUP BY 1,2,3,5) as priorYear

ON currentYear.submissionDate = priorYear.PYDate

))

SELECT * FROM data