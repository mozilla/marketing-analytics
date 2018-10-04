WITH alldata as(
SELECT
sessionsTable.date,
country.standardizedCountry as country,
fxVersionTable.activeRelease,
sessionsTable.siteSessions,
pageEntryTable.firstRunPageSessions,
pageEntryTable.firefoxPageSessions,
sessionsTable.firefoxComReferralSessions,
sessionsTable.directMediumSessions,
sessionsTable.firefoxBrowserMediumSessions,
sessionsTable.organicMediumSessions,
sessionsTable.referralMediumSessions,
sessionsTable.cpcMediumSessions,
sessionsTable.otherMediumSessions,
sessionsTable.desktopDeviceSessions,
sessionsTable.mobileDeviceSessions,
sessionsTable.tabletDeviceSessions,
hitsTable.downloads,
hitsTable.nonFXDownloads,
nonReleaseFxDownloads.FxBetaDownloads,
nonReleaseFxDownloads.FxDeveloperDownloads,
nonReleaseFxDownloads.FxNightlyDownloads,
adjust.installsAndroid,
adjust.installsIOS,
adjust.installsFocusAndroid,
adjust.installsFocusIOS,
adjust.dauAndroid,
adjust.dauIOS,
adjust.dauFocusAndroid,
adjust.dauFocusIOS

FROM(

-- session level metrics --
(SELECT
  date as date,
  geoNetwork.country as country,
  SUM(totals.visits) AS siteSessions,
  SUM(CASE WHEN trafficSource.medium = 'referral' AND trafficSource.source = 'firefox-com' THEN totals.visits ELSE 0 END) as firefoxComReferralSessions,
  SUM(CASE WHEN trafficSource.medium IN ('(not set)', '(none)') THEN totals.visits ELSE 0 END) as directMediumSessions,
  SUM(CASE WHEN trafficSource.medium = 'firefox-browser' THEN totals.visits ELSE 0 END) as firefoxBrowserMediumSessions,
  SUM(CASE WHEN trafficSource.medium = 'organic' THEN totals.visits ELSE 0 END) as organicMediumSessions,
  SUM(CASE WHEN trafficSource.medium = 'referral' THEN totals.visits ELSE 0 END) as referralMediumSessions,
  SUM(CASE WHEN trafficSource.medium = 'cpc' THEN totals.visits ELSE 0 END) as cpcMediumSessions,
  SUM(CASE WHEN trafficSource.medium NOT IN ('(not set)','(none)','firefox-browser','organic','referral','cpc') THEN totals.visits ELSE 0 END) as otherMediumSessions,
  SUM(CASE WHEN device.deviceCategory = 'desktop' THEN totals.visits ELSE 0 END) as desktopDeviceSessions,
  SUM(CASE WHEN device.deviceCategory = 'mobile' THEN totals.visits ELSE 0 END) as mobileDeviceSessions,
  SUM(CASE WHEN device.deviceCategory = 'tablet' THEN totals.visits ELSE 0 END) as tabletDeviceSessions
FROM
  `ga-mozilla-org-prod-001.65789850.ga_sessions_*`
WHERE
_TABLE_SUFFIX >= '20160101'
AND _TABLE_SUFFIX <= 'TODAY()-1'
GROUP By 1,2
) AS sessionsTable

LEFT JOIN

-- hits based metrics --
(SELECT
  date as date,
  country as country,
  SUM(IF(downloads > 0,1,0)) as downloads,
  SUM(IF(downloads > 0 AND browser != 'Firefox',1,0)) as nonFXDownloads
FROM (SELECT
  date AS date,
  fullVisitorId as visitorId,
  visitNumber as visitNumber,
  device.browser as browser,
  geoNetwork.country AS country,
  SUM(IF (hits.eventInfo.eventAction = "Firefox Download",1,0)) as downloads
FROM
  `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
  UNNEST (hits) AS hits
WHERE
  _TABLE_SUFFIX >= '20160101'
  AND _TABLE_SUFFIX <= 'TODAY()-1'
  AND hits.type = 'EVENT'
  AND hits.eventInfo.eventCategory IS NOT NULL
  AND hits.eventInfo.eventLabel LIKE "Firefox for Desktop%"
GROUP BY
  1,
  2,
  3,
  4,
  5)
GROUP BY 1,2) as hitsTable
ON sessionsTable.date = hitsTable.date AND sessionsTable.country = hitsTable.country)

LEFT JOIN
-- Fx Download for non-release versions

(SELECT
  date AS date,
  country AS country,
  SUM(CASE WHEN eventLabel = "Firefox Beta" THEN IF(downloads > 0,1,0) ELSE 0 END) AS FxBetaDownloads,
  SUM(CASE WHEN eventLabel = "Firefox Developer Edition" THEN IF(downloads > 0,1,0) ELSE 0 END) AS FxDeveloperDownloads,
  SUM(CASE WHEN eventLabel = "Firefox Nightly Edition" THEN IF(downloads > 0,1,0) ELSE 0 END) AS FxNightlyDownloads
FROM (
  SELECT
    date AS date,
    fullVisitorId AS visitorId,
    hits.eventInfo.eventLabel AS eventLabel,
    hits.eventInfo.eventCategory AS eventCategory,
    visitNumber AS visitNumber,
    device.browser AS browser,
    geoNetwork.country AS country,
    SUM(IF (hits.eventInfo.eventAction = "Firefox Download", 1, 0)) AS downloads
  FROM
    `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
    UNNEST (hits) AS hits
  WHERE
    _TABLE_SUFFIX >= '20160101'
    AND _TABLE_SUFFIX <= 'TODAY()-1'
    AND hits.type = 'EVENT'
    AND hits.eventInfo.eventCategory IS NOT NULL
    AND hits.eventInfo.eventLabel IN ("Firefox Beta", "Firefox Developer Edition", "Firefox Nightly Edition")
  GROUP BY
    1,2,3,4,5,6,7)
GROUP BY
  1, 2) as nonReleaseFxDownloads
ON sessionsTable.date = nonReleaseFxDownloads.date AND sessionsTable.country = nonReleaseFxDownloads.country


LEFT JOIN

-- Fx Version
(SELECT REPLACE(SUBSTR(STRING(datefield),0,10),"-","") as date, activeRelease as activeRelease
FROM `lookupTables.fxVersionsByDate`
WHERE
dateField >= '2016-01-01') as fxVersionTable
ON sessionsTable.date = fxVersionTable.date

LEFT JOIN

-- /Firstrun and /Firefox sessions
(SELECT
  date AS date,
  geoNetwork.country AS country,
  SUM(CASE WHEN REGEXP_CONTAINS(hits.page.pagePath, '/firstrun/') THEN totals.visits ELSE 0 END) AS firstrunPageSessions,
  SUM(CASE WHEN REGEXP_CONTAINS(hits.page.pagePath, '/firefox/') THEN totals.visits ELSE 0 END) AS firefoxPageSessions
FROM
  `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
  UNNEST (hits) AS hits
WHERE
  _TABLE_SUFFIX >= '20160101'
  AND _TABLE_SUFFIX <= 'TODAY()-1'
  AND hits.isEntrance IS TRUE
GROUP BY
  1,
  2) as pageEntryTable

ON sessionsTable.date = pageEntryTable.date AND sessionsTable.country = pageEntryTable.country

LEFT JOIN

-- Standardize countries
(SELECT rawCountry, standardizedCountry FROM `ga-mozilla-org-prod-001.lookupTables.standardizedCountryList`) as country
ON sessionsTable.country = country.rawCountry

LEFT JOIN

-- Adjust Data. Clean up country prior to joining with GA
(SELECT
adjustRaw.date,
adjustcountry.standardizedCountry,
adjustRaw.installsAndroid,
adjustRaw.installsIOS,
adjustRaw.installsFocusAndroid,
adjustRaw.installsFocusIOS,
adjustRaw.dauAndroid,
adjustRaw.dauIOS,
adjustRaw.dauFocusAndroid,
adjustRaw.dauFocusIOS
FROM
(SELECT
  FORMAT_DATE('%Y%m%d', date) AS date,
  country as country,
  SUM(CASE WHEN app = 'Firefox Android and iOS' AND os = 'android' THEN installs ELSE 0 END) AS installsAndroid,
  SUM(CASE WHEN app = 'Firefox Android and iOS' AND os = 'ios' THEN installs ELSE 0 END) AS installsIOS,
  SUM(CASE WHEN app = 'Focus by Firefox - content blocking' AND os = 'android' THEN installs ELSE 0 END) AS installsFocusAndroid,
  SUM(CASE WHEN app = 'Focus by Firefox - content blocking' AND os = 'ios' THEN installs ELSE 0 END) AS installsFocusIOS,
  SUM(CASE WHEN app = 'Firefox Android and iOS' AND os = 'android' THEN daus ELSE 0 END) AS dauAndroid,
  SUM(CASE WHEN app = 'Firefox Android and iOS' AND os = 'ios' THEN daus ELSE 0 END) AS dauIOS,
  SUM(CASE WHEN app = 'Focus by Firefox - content blocking' AND os = 'android' THEN daus ELSE 0 END) AS dauFocusAndroid,
  SUM(CASE WHEN app = 'Focus by Firefox - content blocking' AND os = 'ios' THEN daus ELSE 0 END) AS dauFocusIOS
FROM
  `ga-mozilla-org-prod-001.Adjust.deliverable_*`
WHERE
  _TABLE_SUFFIX >= '20160101'
GROUP BY
  1,2
) as adjustRaw

LEFT JOIN

(SELECT rawCountry, standardizedCountry FROM `ga-mozilla-org-prod-001.lookupTables.standardizedCountryList`) as adjustCountry

ON adjustRaw.country = adjustCountry.rawCountry) as adjust

ON adjust.date = sessionsTable.date AND adjust.standardizedCountry = country.standardizedCountry
)

--- Pull from all data table

SELECT * FROM alldata