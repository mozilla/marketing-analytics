SELECT
  date,
  SUM(CASE WHEN app = 'Firefox Android and iOS' AND os = 'android' THEN daus ELSE 0 END) as androidDAU,
  SUM(CASE WHEN app = 'Firefox Android and iOS' AND os = 'ios' THEN daus ELSE 0 END) as iosDAU,
  SUM(CASE WHEN app = 'Focus by Firefox - content blocking' AND os = 'ios' THEN daus ELSE 0 END) as iosFocusDAU,
  SUM(CASE WHEN app = 'Focus by Firefox - content blocking' AND os = 'android' THEN daus ELSE 0 END) as androidFocusDAU,
  SUM(CASE WHEN app = 'Firefox Rocket' THEN daus ELSE 0 END) as rocketDAU
FROM `ga-mozilla-org-prod-001.Adjust.deliverable_*`
WHERE date > '2018-10-01'
GROUP BY 1
ORDER BY 1