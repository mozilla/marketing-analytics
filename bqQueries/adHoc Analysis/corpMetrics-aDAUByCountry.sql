-- used to pull data by country from CorpMetrics

SELECT
submission_date_s3,
sum(activeDAU) as totalaDAU,
SUM(CASE WHEN countryName = 'United Kingdom' THEN activeDAU ELSE 0 END) as aDAUGB,
SUM(CASE WHEN countryName = 'Germany' THEN activeDAU ELSE 0 END) as aDAUDE,
SUM(CASE WHEN countryName = 'Canada' THEN activeDAU ELSE 0 END) as aDAUCA,
SUM(CASE WHEN countryName = 'USA' THEN activeDAU ELSE 0 END) as aDAUUS,
SUM(CASE WHEN countryName = 'France' THEN activeDAU ELSE 0 END) as aDAUFR
FROM
  `ga-mozilla-org-prod-001.telemetry.corpMetrics`
GROUP BY 1
ORDER BY 1