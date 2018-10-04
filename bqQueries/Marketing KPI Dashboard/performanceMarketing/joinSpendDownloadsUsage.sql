WITH data AS(
SELECT
*

FROM(
(-- Select fetch spend
SELECT
  FORMAT_DATE('%Y%m%d', date) as date,
  Adname as adNameFetch,
  campaign as campaignFetch,
  channel as channelFetch,
  SUM(vendorNetSpend) AS vendorNetSpend
FROM
  `ga-mozilla-org-prod-001.fetch.fetch_deduped`
WHERE
  channel IN ('Display','Search','SOCIAL')
GROUP BY 1,2,3,4
ORDER BY 5 DESC) as Ftch

LEFT JOIN

-- Select Downloads
(SELECT
  date as dateGA,
  source as sourceGA,
  medium as mediumGA,
  campaign as campaignGA,
  adcontent as adContentGA,
  SUM(IF(downloads > 0,1,0)) as downloads,
  SUM(IF(downloads > 0 AND browser != 'Firefox',1,0)) as nonFXDownloads
FROM (SELECT
  date AS date,
  trafficSource.adcontent as adContent,
  trafficSource.source as source,
  trafficSource.medium as medium,
  trafficSource.campaign as campaign,
  device.browser as browser,
  SUM(IF (hits.eventInfo.eventAction = "Firefox Download",1,0)) as downloads
FROM
  `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
  UNNEST (hits) AS hits
WHERE
  _TABLE_SUFFIX >= '20170101'
  AND hits.type = 'EVENT'
  AND hits.eventInfo.eventCategory IS NOT NULL
  AND hits.eventInfo.eventLabel LIKE "Firefox for Desktop%"
GROUP BY
  1,2,3,4,5,6)
GROUP BY 1,2,3,4,5
ORDER BY 1) as GA

ON ftch.date = GA.dateGA AND ftch.AdnameFetch = GA.adContentGA

LEFT JOIN

-- Select Usage
(SELECT
  submission_date_s3 as dateTelem,
  contentCleaned as contentTelem,
  SUM(installs) AS installs,
  SUM(DAU) AS DAU,
  SUM(activeDAU) AS aDAU,
  SUM(searches) AS searches
FROM
  `ga-mozilla-org-prod-001.telemetry.corpMetrics`
GROUP BY 1,2
ORDER BY 3 DESC) as usage

ON ftch.date = usage.dateTelem AND ftch.AdnameFetch = usage.contentTelem))

SELECT * FROM data