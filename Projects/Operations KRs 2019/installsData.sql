WITH corpMetrics as(
SELECT
    FORMAT_DATE("%Y%m", submission) AS month,
    CASE
      WHEN country = 'CA' THEN 'Canada'
    ELSE CASE
      WHEN country = 'FR' THEN 'France'
    ELSE CASE
      WHEN country = 'DE' THEN 'Germany'
    ELSE CASE
      WHEN country = 'GB' THEN 'United Kingdom'
    ELSE CASE
      WHEN country = 'US' THEN 'United States'
    ELSE 'rest of world'
    END END
    END END
    END AS country,
    CASE
      WHEN LOWER(mediumCleaned) = 'organic' THEN 'organic'
      ELSE CASE
      WHEN LOWER(mediumCleaned) IN ('banner',  'cpc',  'display',  'paidsearch',  'ppc',  'social',  'video') THEN 'paid'
      ELSE CASE
      WHEN LOWER(mediumCleaned) IN ('blog',  'download_button',  'email',  'firefox-browser',  'fx-accounts',  'lp',  'native',  'show-heartbeat',  'snippet',  'static',  'tiles') THEN 'owned'
      ELSE CASE
      WHEN LOWER(mediumCleaned) = '(none)' AND sourceCleaned = '(direct)' THEN 'direct'
      ELSE CASE
      WHEN LOWER(mediumCleaned) = 'referral' THEN 'referral'
      ELSE 'other'
    END END
    END END
    END AS channels,
    SUM(installs) AS totalInstalls,
    SUM(CASE WHEN funnelOrigin = 'darkFunnel' THEN installs ELSE 0 END) as darkFunnelInstalls,
    SUM(CASE WHEN funnelOrigin = 'partnerships' THEN installs ELSE 0 END) as partnershipInstalls,
    SUM(CASE
        WHEN funnelOrigin = 'mozFunnel' THEN installs
        ELSE 0 END) AS mktgAttrInstalls
  FROM
    `ga-mozilla-org-prod-001.desktop.desktop_corp_metrics_*`
  WHERE
    submission >= "2018-01-01" 
    AND submission <= "2019-05-18"
  GROUP BY 1,2,3
  ORDER BY 1,2,5 DESC),
  
adjustedCorpMetrics as (
SELECT
    FORMAT_DATE("%Y%m", submission) AS month,
    CASE
      WHEN country = 'CA' THEN 'Canada'
    ELSE CASE
      WHEN country = 'FR' THEN 'France'
    ELSE CASE
      WHEN country = 'DE' THEN 'Germany'
    ELSE CASE
      WHEN country = 'GB' THEN 'United Kingdom'
    ELSE CASE
      WHEN country = 'US' THEN 'United States'
    ELSE 'rest of world'
    END END
    END END
    END AS country,
    CASE
      WHEN LOWER(mediumCleaned) = 'organic' THEN 'organic'
      ELSE CASE
      WHEN LOWER(mediumCleaned) IN ('banner',  'cpc',  'display',  'paidsearch',  'ppc',  'social',  'video') THEN 'paid'
      ELSE CASE
      WHEN LOWER(mediumCleaned) IN ('blog',  'download_button',  'email',  'firefox-browser',  'fx-accounts',  'lp',  'native',  'show-heartbeat',  'snippet',  'static',  'tiles') THEN 'owned'
      ELSE CASE
      WHEN LOWER(mediumCleaned) = '(none)' AND sourceCleaned = '(direct)' THEN 'direct'
      ELSE CASE
      WHEN LOWER(mediumCleaned) = 'referral' THEN 'referral'
      ELSE 'other'
    END END
    END END
    END AS channels,
    SUM(installs) AS totalInstalls,
    SUM(CASE WHEN funnelOrigin = 'darkFunnel' THEN installs ELSE 0 END) as darkFunnelInstalls,
    SUM(CASE WHEN funnelOrigin = 'partnerships' THEN installs ELSE 0 END) as partnershipInstalls,
    SUM(CASE
        WHEN funnelOrigin = 'mozFunnel' THEN installs
        ELSE 0 END) AS mktgAttrInstalls
  FROM
    `ga-mozilla-org-prod-001.desktop.desktop_corpMetrics_post_profilePerInstallAdj_*`
  WHERE
    submission >= "2019-05-19" 
    AND submission <= "2019-06-30"
  GROUP BY 1,2,3
  ORDER BY 1,2,5 DESC),
  
aggregated as (
  
-- Select installs by country
SELECT
  PARSE_DATE('%Y%m%d', CONCAT(month,'01')) as period,
  month,
  country,
  channels,
  SUM(mktgAttrInstalls) as mktgAttrInstalls,
  SUM(darkFunnelInstalls) as darkFunnelInstalls,
  SUM(partnershipInstalls) as partnershipInstalls,
  SUM(totalInstalls) as totalInstalls
FROM
  corpMetrics
GROUP BY
  period, month, country, channels
  
UNION ALL
SELECT
  PARSE_DATE('%Y%m%d', CONCAT(month,'01')) as period,
  month,
  country,
  channels,
  SUM(mktgAttrInstalls) as mktgAttrInstalls,
  SUM(darkFunnelInstalls) as darkFunnelInstalls,
  SUM(partnershipInstalls) as partnershipInstalls,
  SUM(totalInstalls) as totalInstalls
FROM
  adjustedCorpMetrics
GROUP BY
  period, month, country, channels
  
UNION ALL

-- SELECT installs total
SELECT
  PARSE_DATE('%Y%m%d', CONCAT(month,'01')) as period,
  month,
  'total' as country,
  channels,
  SUM(mktgAttrInstalls) as mktgAttrInstalls,
  SUM(darkFunnelInstalls) as darkFunnelInstalls,
  SUM(partnershipInstalls) as partnershipInstalls,
  SUM(totalInstalls) as totalInstalls
FROM
  corpMetrics
GROUP BY
  period, month, country, channels
  
UNION ALL
SELECT
  PARSE_DATE('%Y%m%d', CONCAT(month,'01')) as period,
  month,
  'total' as country,
  channels,
  SUM(mktgAttrInstalls) as mktgAttrInstalls,
  SUM(darkFunnelInstalls) as darkFunnelInstalls,
  SUM(partnershipInstalls) as partnershipInstalls,
  SUM(totalInstalls) as totalInstalls
FROM
  adjustedCorpMetrics
GROUP BY
  period, month, country, channels
  
UNION ALL

-- Select installs for tier 1 aggregated
SELECT
  PARSE_DATE('%Y%m%d', CONCAT(month,'01')) as period,
  month,
  'tier 1' as country,
  channels,
  SUM(mktgAttrInstalls) as mktgAttrInstalls,
  SUM(darkFunnelInstalls) as darkFunnelInstalls,
  SUM(partnershipInstalls) as partnershipInstalls,
  SUM(totalInstalls) as totalInstalls
FROM
  corpMetrics
WHERE
  country IN ('Canada',  'France',  'Germany',  'United Kingdom',  'United States')
GROUP BY
  period, month, country, channels
  
UNION ALL
SELECT
  PARSE_DATE('%Y%m%d', CONCAT(month,'01')) as period,
  month,
  'tier 1' as country,
  channels,
  SUM(mktgAttrInstalls) as mktgAttrInstalls,
  SUM(darkFunnelInstalls) as darkFunnelInstalls,
  SUM(partnershipInstalls) as partnershipInstalls,
  SUM(totalInstalls) as totalInstalls
FROM
  adjustedCorpMetrics
WHERE
  country IN ('Canada',  'France',  'Germany',  'United Kingdom',  'United States')
GROUP BY
  period, month, country, channels

UNION ALL

-- SELECT Tier 1 NA aggregate

SELECT
  PARSE_DATE('%Y%m%d', CONCAT(month,'01')) as period,
  month,
  'tier 1-NA' as country,
  channels,
  SUM(mktgAttrInstalls) as mktgAttrInstalls,
  SUM(darkFunnelInstalls) as darkFunnelInstalls,
  SUM(partnershipInstalls) as partnershipInstalls,
  SUM(totalInstalls) as totalInstalls
FROM
  corpMetrics
WHERE
  country IN ('Canada', 'United States')
GROUP BY
  period, month, country, channels
  
UNION ALL
SELECT
  PARSE_DATE('%Y%m%d', CONCAT(month,'01')) as period,
  month,
  'tier 1-NA' as country,
  channels,
  SUM(mktgAttrInstalls) as mktgAttrInstalls,
  SUM(darkFunnelInstalls) as darkFunnelInstalls,
  SUM(partnershipInstalls) as partnershipInstalls,
  SUM(totalInstalls) as totalInstalls
FROM
  adjustedCorpMetrics
WHERE
  country IN ('Canada', 'United States')
GROUP BY
  period, month, country, channels
  
UNION ALL

-- Select Tier 1 EU aggregate
SELECT
  PARSE_DATE('%Y%m%d', CONCAT(month,'01')) as period,
  month,
  'tier 1-EU' as country,
  channels,
  SUM(mktgAttrInstalls) as mktgAttrInstalls,
  SUM(darkFunnelInstalls) as darkFunnelInstalls,
  SUM(partnershipInstalls) as partnershipInstalls,
  SUM(totalInstalls) as totalInstalls
FROM
  corpMetrics
WHERE
  country IN ('France',  'Germany',  'United Kingdom')
GROUP BY
  period, month, country, channels
  
UNION ALL
SELECT
  PARSE_DATE('%Y%m%d', CONCAT(month,'01')) as period,
  month,
  'tier 1-EU' as country,
  channels,
  SUM(mktgAttrInstalls) as mktgAttrInstalls,
  SUM(darkFunnelInstalls) as darkFunnelInstalls,
  SUM(partnershipInstalls) as partnershipInstalls,
  SUM(totalInstalls) as totalInstalls
FROM
  adjustedCorpMetrics
WHERE
  country IN ('France',  'Germany',  'United Kingdom')
GROUP BY
  period, month, country, channels)
  
SELECT 
  period,
  month,
  country,
  channels,
  SUM(totalInstalls) as totalInstalls,
  SUM(mktgAttrInstalls) as mktgAttrInstalls,
  SUM(darkFunnelInstalls) as darkFunnelInstalls,
  SUM(partnershipInstalls) as partnershipInstalls
FROM aggregated
  GROUP BY period, month, country, channels
  ORDER BY period, month, country, mktgAttrInstalls DESC