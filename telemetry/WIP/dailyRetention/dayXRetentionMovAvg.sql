-- Used in dashboard https://datastudio.google.com/reporting/153_dIAClfvYQ1woP-YTtO3cR6Rqt-3yJ/page/wN8V/edit

SELECT
  submission_date_s3,
  acqSegmentCleaned,
  installDate,
  daysRetained,
  installs,
  ROUND(AVG(installs) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS installs28DAYAvg,
  DAU,
  ROUND(AVG(DAU) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS DAU28DAY,
  activeDAU,
  ROUND(AVG(activeDAU) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS aDAU28DAYAvg,
  retention,
  ROUND(AVG(retention) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW),4) AS retention28DAYAvg
FROM
  `ga-mozilla-org-prod-001.telemetry.dailyRetentionWithInstallsv2`
WHERE
  acqSegmentCleaned = 'new2018'
AND daysRetained = 1
GROUP BY
  1,2,3,4,5,7,9,11

UNION ALL

SELECT
  submission_date_s3,
  acqSegmentCleaned,
  installDate,
  daysRetained,
  installs,
  ROUND(AVG(installs) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS installs28DAYAvg,
  DAU,
  ROUND(AVG(DAU) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS DAU28DAY,
  activeDAU,
  ROUND(AVG(activeDAU) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS aDAU28DAYAvg,
  retention,
  ROUND(AVG(retention) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW),4) AS retention28DAYAvg
FROM
  `ga-mozilla-org-prod-001.telemetry.dailyRetentionWithInstallsv2`
WHERE
  acqSegmentCleaned = 'new2018'
AND daysRetained = 7
GROUP BY
  1,2,3,4,5,7,9,11

UNION ALL

SELECT
  submission_date_s3,
  acqSegmentCleaned,
  installDate,
  daysRetained,
  installs,
  ROUND(AVG(installs) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS installs28DAYAvg,
  DAU,
  ROUND(AVG(DAU) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS DAU28DAY,
  activeDAU,
  ROUND(AVG(activeDAU) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS aDAU28DAYAvg,
  retention,
  ROUND(AVG(retention) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW),4) AS retention28DAYAvg
FROM
  `ga-mozilla-org-prod-001.telemetry.dailyRetentionWithInstallsv2`
WHERE
  acqSegmentCleaned = 'new2018'
AND daysRetained = 14
GROUP BY
  1,2,3,4,5,7,9,11

UNION ALL

SELECT
  submission_date_s3,
  acqSegmentCleaned,
  installDate,
  daysRetained,
  installs,
  ROUND(AVG(installs) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS installs28DAYAvg,
  DAU,
  ROUND(AVG(DAU) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS DAU28DAY,
  activeDAU,
  ROUND(AVG(activeDAU) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS aDAU28DAYAvg,
  retention,
  ROUND(AVG(retention) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW),4) AS retention28DAYAvg
FROM
  `ga-mozilla-org-prod-001.telemetry.dailyRetentionWithInstallsv2`
WHERE
  acqSegmentCleaned = 'new2018'
AND daysRetained = 28
GROUP BY
  1,2,3,4,5,7,9,11