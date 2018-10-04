-- Query used to map out aDAU by country on aDAU dashboard
WITH data AS(
SELECT
currentYear.submissionDate,
currentYear.country,
currentYear.countryName,
currentYear.DAU,
currentYear.DAUMetric,
currentYear.aDAU,
currentYear.aDAUMetric,
IFNULL(priorYear.DAUMetric,0) as pyDAUMetric,
IFNULL(priorYear.aDAUMetric,0) as pyaDAUMetric,
IFNULL(SAFE_DIVIDE(currentYear.DAUMetric, priorYear.DAUMetric)-1,0) as dauMetricYOYVar,
IFNULL(SAFE_DIVIDE(currentYear.aDAUMetric, priorYear.aDAUMetric)-1,0) as aDauMetricYOYVar
FROM (
(SELECT
submissionDate as submissionDate,
country,
countryName,
DAU,
ROUND(AVG(DAU) OVER (ORDER BY countryName, country, submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS DAUMetric,
aDAU,
ROUND(AVG(aDAU) OVER (ORDER BY countryName, country, submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS aDAUMetric
FROM (
  SELECT
    submission_date_s3 AS submissionDate,
    country as country,
    countryName as countryName,
    sum(DAU) as DAU,
    sum(activeDAU) as aDAU,
    sum(installs) as installs,
    sum(searches) as searches
  FROM
    `ga-mozilla-org-prod-001.telemetry.corpMetrics`
  GROUP By 1,2,3)
GROUP BY 1,2,3,4,6) as currentYear

LEFT JOIN
(SELECT
submissionDate as submissionDate,
FORMAT_DATE('%Y%m%d', DATE_ADD(PARSE_DATE('%Y%m%d', submissionDate), INTERVAL 1 YEAR)) as PYDate,
country,
countryName,
DAU,
ROUND(AVG(DAU) OVER (ORDER BY countryName, country, submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS DAUMetric,
aDAU,
ROUND(AVG(aDAU) OVER (ORDER BY countryName, country, submissionDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS aDAUMetric
FROM (
  SELECT
    submission_date_s3 AS submissionDate,
    country as country,
    countryName as countryName,
    sum(DAU) as DAU,
    sum(activeDAU) as aDAU,
    sum(installs) as installs,
    sum(searches) as searches
  FROM
    `ga-mozilla-org-prod-001.telemetry.corpMetrics`
  GROUP By 1,2,3)
GROUP BY 1,2,3,4,5,7) as priorYear

ON currentYear.submissionDate = priorYear.PYDate AND currentYear.country = priorYear.country

))

SELECT * FROM data