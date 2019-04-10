WITH data as(
SELECT
submission,
funnelOrigin,
CASE WHEN country IN ('US', 'CA', 'DE', 'GB', 'FR') THEN country ELSE 'restOfWorld' END as country,
SUM(installs) as installs,
SUM(dau) as dau,
SUM(wau) as wau,
SUM(mau) as mau
FROM
  `ga-mozilla-org-prod-001.desktop.desktop_corp_metrics_*`
WHERE
  _TABLE_SUFFIX >= '20170901'
GROUP BY submission, country, funnelOrigin),

totals as (
SELECT
  submission,
  'total' as funnelOrigin,
  'total' as country,
  SUM(installs) as installs,
  SUM(dau) as dau,
  SUM(wau) as wau,
  SUM(mau) as mau
FROM data
GROUP BY 1,2,3),

totalsNA as (
SELECT
  submission,
  'total' as funnelOrigin,
  'tier1NA' as country,
  SUM(installs) as installs,
  SUM(dau) as dau,
  SUM(wau) as wau,
  SUM(mau) as mau
FROM data
WHERE country IN ('US', 'CA')
GROUP BY 1,2,3),

NAByFunnel as (
SELECT
  submission,
  funnelOrigin,
  'tier1NA' as country,
  SUM(installs) as installs,
  SUM(dau) as dau,
  SUM(wau) as wau,
  SUM(mau) as mau
FROM data
WHERE country IN ('US', 'CA')
GROUP BY 1,2,3),

totalsEU as (
SELECT
  submission,
  'total' as funnelOrigin,
  'tier1EU' as country,
  SUM(installs) as installs,
  SUM(dau) as dau,
  SUM(wau) as wau,
  SUM(mau) as mau
FROM data
WHERE country IN ('DE', 'FR', 'GB')
GROUP BY 1,2,3),

EUByFunnel as (
SELECT
  submission,
  funnelOrigin,
  'tier1EU' as country,
  SUM(installs) as installs,
  SUM(dau) as dau,
  SUM(wau) as wau,
  SUM(mau) as mau
FROM data
WHERE country IN ('DE', 'FR', 'GB')
GROUP BY 1,2,3),

totalsByCountry as (
SELECT
  submission,
  'total' as funnelOrigin,
  country,
  SUM(installs) as installs,
  SUM(dau) as dau,
  SUM(wau) as wau,
  SUM(mau) as mau
FROM data
GROUP BY 1,2,3)


-- Total Country By Total Funnel
SELECT
  submission,
  funnelOrigin,
  country,
  installs,
  dau,
  wau,
  mau,
  ROUND(AVG(installs) OVER (ORDER BY submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS installs7DayAvg,
  ROUND(AVG(dau) OVER (ORDER BY submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS dau7DayAvg
FROM totals
GROUP BY 1,2,3,4,5,6,7

UNION ALL

-- Total NA
SELECT
  submission,
  funnelOrigin,
  country,
  installs,
  dau,
  wau,
  mau,
  ROUND(AVG(installs) OVER (ORDER BY submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS installs7DayAvg,
  ROUND(AVG(dau) OVER (ORDER BY submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS dau7DayAvg
FROM totalsNA
GROUP BY 1,2,3,4,5,6,7

UNION ALL

-- NA By Funnel Type
SELECT
  submission,
  funnelOrigin,
  country,
  installs,
  dau,
  wau,
  mau,
  ROUND(AVG(installs) OVER (ORDER BY funnelOrigin, submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS installs7DayAvg,
  ROUND(AVG(dau) OVER (ORDER BY funnelOrigin, submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS dau7DayAvg
FROM NAByFunnel
GROUP BY 1,2,3,4,5,6,7

UNION ALL

-- Total EU
SELECT
  submission,
  funnelOrigin,
  country,
  installs,
  dau,
  wau,
  mau,
  ROUND(AVG(installs) OVER (ORDER BY submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS installs7DayAvg,
  ROUND(AVG(dau) OVER (ORDER BY submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS dau7DayAvg
FROM totalsEU
GROUP BY 1,2,3,4,5,6,7

UNION ALL

-- EU By Funnel Type
SELECT
  submission,
  funnelOrigin,
  country,
  installs,
  dau,
  wau,
  mau,
  ROUND(AVG(installs) OVER (ORDER BY funnelOrigin, submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS installs7DayAvg,
  ROUND(AVG(dau) OVER (ORDER BY funnelOrigin, submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS dau7DayAvg
FROM EUByFunnel
GROUP BY 1,2,3,4,5,6,7

UNION ALL

-- Totals By country
SELECT
  submission,
  funnelOrigin,
  country,
  installs,
  dau,
  wau,
  mau,
  ROUND(AVG(installs) OVER (ORDER BY country, submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS installs7DayAvg,
  ROUND(AVG(dau) OVER (ORDER BY country, submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS dau7DayAvg
FROM totalsByCountry
GROUP BY 1,2,3,4,5,6,7

UNION ALL

-- Funnels by Country
SELECT
  submission,
  funnelOrigin,
  country,
  installs,
  dau,
  wau,
  mau,
  ROUND(AVG(installs) OVER (ORDER BY country, funnelOrigin, submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS installs7DayAvg,
  ROUND(AVG(dau) OVER (ORDER BY country, funnelOrigin, submission ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS dau7DayAvg
FROM data
GROUP BY 1,2,3,4,5,6,7