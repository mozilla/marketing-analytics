WITH data as(
SELECT
submission,
SUM(installs) as installs,
SUM(dau) as dau,
SUM(wau) as wau,
SUM(mau) as mau
FROM
  `ga-mozilla-org-prod-001.desktop.desktop_corp_metrics_*`
WHERE
  _TABLE_SUFFIX >= '20170901'
GROUP BY 1)

SELECT
  submission,
  installs,
  dau,
  wau,
  mau,
  ROUND(AVG(installs) OVER (ORDER BY submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS installs7DayAvg,
  ROUND(AVG(dau) OVER (ORDER BY submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS dau7DayAvg
FROM data
GROUP BY 1,2,3,4,5