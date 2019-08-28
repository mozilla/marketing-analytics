-- Query used to join installs to website metrics at a detailed level ie. across source, medium, campaign, content and country

WITH parameters as(
SELECT
'20190401' as startDate,
'20190430' as endDate
),

-- Pull websites data
websiteData as(
SELECT
  *,
  CASE
    WHEN medium = 'organic' THEN 'organic'
    ELSE CASE
    WHEN medium IN ('banner',  'cpc',  'display',  'paidsearch',  'ppc',  'social',  'video') THEN 'paid'
    ELSE CASE
    WHEN medium IN ('blog',  'download_button',  'email',  'firefox-browser',  'fx-accounts',  'lp',  'native',  'show-heartbeat',  'snippet',  'static',  'tiles') THEN 'owned'
    ELSE CASE
    WHEN medium = '(none)' AND source = '(direct)' THEN 'direct'
    ELSE CASE
    WHEN medium = 'referral' THEN 'referral'
    ELSE 'other'
  END END
  END END
  END AS channelGrouping
FROM
  `ga-mozilla-org-prod-001.desktop.website_metrics_*`
WHERE
  _TABLE_SUFFIX >= (SELECT startDate FROM parameters)
  AND _TABLE_SUFFIX <= (SELECT endDate FROM parameters)),

-- Pull install data
installData as (
SELECT
  submission,
  countryName,
  sourceCleaned,
  mediumCleaned,
  campaignCleaned,
  contentCleaned,
  SUM(installs) as installs
FROM `desktop.desktop_corp_metrics_*`
WHERE
  _TABLE_SUFFIX >= (SELECT startDate FROM parameters)
  AND _TABLE_SUFFIX <= (SELECT endDate FROM parameters)
  AND funnelOrigin = 'mozFunnel'
GROUP BY
  submission,countryName, sourceCleaned, mediumCleaned, campaignCleaned, contentCleaned),

-- Join install data to website data to get installs
joinedData as (
SELECT
  websiteData.date,
  websiteData.countryCleaned,
  websiteData.source,
  websiteData.medium,
  websiteData.campaign,
  websiteData.content,
  websiteData.channelGrouping,
  websiteData.sessions,
  websiteData.nonFXSessions,
  websiteData.downloads,
  websiteData.nonFXDownloads,
  installData.installs
FROM
  websiteData
FULL JOIN
  installData
ON
  websiteData.date = installData.submission
  AND websiteData.countryCleaned = installData.countryName
  AND websiteData.source = installData.sourceCleaned
  AND websiteData.medium = installData.mediumCleaned
  AND websiteData.campaign = installData.campaignCleaned
  AND websiteData.content = installData.contentCleaned
)

SELECT
*
FROM
joinedData
WHERE channelGrouping IS NULL
AND INSTALLS != 0
ORDER BY installs DESC

/*channelGrouping,
SUM(nonFXSessions) as nonFXSessions,
SUM(nonFXDownloads) as nonFXDownloads,
SUM(installs) as installs
FROM joinedData
GROUP BY 1
ORDER BY 3 DESC*/

-- Left off with issues matching up organic, other and referral
-- See working doc: https://docs.google.com/spreadsheets/d/1Vnt6eaY54VbFTkzeKeQ1ocKQbS-C9i3ktRDYPkhJ9l0/edit#gid=2019777242
-- For trailhead join at the channel grouping level