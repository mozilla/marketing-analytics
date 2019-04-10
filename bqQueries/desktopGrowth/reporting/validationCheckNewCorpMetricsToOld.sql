WITH newtable as(
SELECT
mediumCleaned,
SUM(dau) as dau,
SUM(installs) as installs
FROM
  `ga-mozilla-org-prod-001.testDataSet2.desktop_corp_metrics_9_20181101`
GROUP BY 1),

old as(
SELECT
mediumCleaned,
SUM(dau) as dauCorpMetrics,
SUM(installs) as installsCorpMetrics
FROM
  `ga-mozilla-org-prod-001.telemetry.corpMetrics`
WHERE submission_date_s3 = '20181101'
GROUP BY 1)

SELECT
newtable.mediumCleaned,
old.mediumCleaned as mediumCleanedCorpMetrics,
SUM(newtable.dau) as dau,
SUM(old.dauCorpMetrics) as dauCorpMetrics,
SUM(newtable.installs) as installs,
SUM(old.installsCorpMetrics) as installsCorpMetrics
FROM
  newtable
LEFT JOIN
old
ON newtable.mediumCleaned = old.mediumCleaned
GROUP BY 1,2
ORDER BY 3 DESC
