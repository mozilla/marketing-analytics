-- Query used to visualize charts in Fx Desktop Growth report showing breakdown of aDAU by partnerships, mozfunnel and darkFunnel

WITH data AS (
-- converts total to darkFunnel-total for more accurate area map chart
  SELECT
  submission,
  'darkFunnel-total' AS funnelOrigin,
  sum(dau) as dau,
  sum(wau) as wau,
  sum(mau) as mau,
  sum(installs) as installs
FROM
  `ga-mozilla-org-prod-001.desktop.desktop_corp_metrics_*`
WHERE
  _TABLE_SUFFIX >= '20171115'
GROUP By 1,2

UNION ALL
-- Select metrics and create a funnel for mozilla.org funnel
SELECT
  submission,
  funnelOrigin,
  sum(dau) as dau,
  sum(wau) as wau,
  sum(mau) as mau,
  sum(installs) as installs
FROM
  `ga-mozilla-org-prod-001.desktop.desktop_corp_metrics_*`
WHERE
  _TABLE_SUFFIX >= '20171115'
GROUP By 1,2)

SELECT
  submission,
  funnelOrigin,
  dau,
  wau,
  mau,
  installs,
  ROUND(AVG(dau) OVER (ORDER BY funnelOrigin, submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS dau7DayAvg,
  ROUND(AVG(installs) OVER (ORDER BY funnelOrigin, submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS installs7DayAvg
FROM data
GROUP BY 1,2,3,4,5,6