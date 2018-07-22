-- used in corp weekly update to create DAU and installs area charts
WITH data as (SELECT
  date,
  FORMAT_DATE('%U', date) as week,
  CASE
    WHEN app = 'Firefox Android and iOS' AND os = 'android' THEN 'android'
    ELSE CASE
    WHEN app = 'Firefox Android and iOS' AND os = 'ios' THEN 'ios'
    ELSE CASE
    WHEN app = 'Focus by Firefox - content blocking' THEN 'Focus'
    ELSE CASE
    WHEN app = 'Klar by Firefox - content blocker' THEN 'Klar'
    ELSE CASE
    when app = 'Firefox Rocket' THEN 'Rocket'
    ELSE CASE
    WHEN app = 'Pocket' THEN 'Pocket'
  END END END END
  END END as mobileApp,
  SUM(daus) AS dau,
  SUM(installs) AS installs
FROM
  `ga-mozilla-org-prod-001.Adjust.deliverable_*`
WHERE os IN ('android', 'ios')
GROUP BY  1,2,3
ORDER BY 1,2,3)

SELECT
date,
week,
mobileApp,
dau,
installs,
ROUND(AVG(dau) OVER (ORDER BY mobileApp, date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS DAU7DayAvg,
ROUND(AVG(installs) OVER (ORDER BY mobileApp, date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS installs7DayAvg
FROM data
ORDER BY 3,1
