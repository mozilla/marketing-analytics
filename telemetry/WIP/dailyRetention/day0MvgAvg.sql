-- Used in dashboard https://datastudio.google.com/reporting/153_dIAClfvYQ1woP-YTtO3cR6Rqt-3yJ/page/wN8V/edit

SELECT
  submission_date_s3,
  acqSegmentCleaned,
  installDate,
  daysRetained,
  installs,
  ROUND(AVG(installs) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS installs28DAY,
  DAU,
  ROUND(AVG(DAU) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS DAU28DAY,
  activeDAU,
  ROUND(AVG(activeDAU) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)) AS aDAU28DAY,
  retention,
  ROUND(AVG(retention) OVER (ORDER BY daysRetained, submission_date_s3, installDate ROWS BETWEEN 27 PRECEDING AND CURRENT ROW),4) AS retention28DAY
FROM
  `ga-mozilla-org-prod-001.telemetry.dailyRetentionWithInstallsv2`
WHERE
  acqSegmentCleaned = 'new2018'
  AND daysRetained = 0
GROUP BY
  1,2,3,4,5,7,9,11