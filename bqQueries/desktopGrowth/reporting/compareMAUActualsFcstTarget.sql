WITH actuals as (
SELECT
  submission as date,
  SUM(mau) as mau
FROM
  `ga-mozilla-org-prod-001.telemetry.desktopTelemetryDrop`
GROUP BY
  submission),

target as (
SELECT
  date,
  SUM(mau)*1.10 as mauTarget
FROM
  `ga-mozilla-org-prod-001.desktop.desktop_forecast_*`
WHERE
  _TABLE_SUFFIX = (SELECT max(_TABLE_SUFFIX) FROM `ga-mozilla-org-prod-001.desktop.desktop_forecast_*`)
  AND date >= '2019-01-01'
GROUP BY
  date),

forecast as (
SELECT
  date,
  SUM(mau) as mauForecast
FROM
  `ga-mozilla-org-prod-001.desktop.desktop_forecast_*`
WHERE
  _TABLE_SUFFIX = (SELECT max(_TABLE_SUFFIX) FROM `ga-mozilla-org-prod-001.desktop.desktop_forecast_*`)
GROUP BY
  date)

SELECT
  target.date,
  target.mauTarget,
  forecast.mauForecast,
  actuals.mau as mauActuals
FROM
  target
LEFT JOIN
  actuals
ON
  target.date = actuals.date
LEFT JOIN
  forecast
ON
  target.date = forecast.date
ORDER BY
  date
