SELECT
  month,
  CASE
    WHEN country = 'ca' THEN 'Canada'
    ELSE CASE
    WHEN country = 'fr' THEN 'France'
    ELSE CASE
    WHEN country = 'de' THEN 'Germany'
    ELSE CASE
    WHEN country = 'gb' THEN 'United Kingdom'
    ELSE CASE
    WHEN country = 'us' THEN 'United States'
    ELSE country
  END END
  END END
  END AS country,
  channels,
  SUM(totalInstalls) AS totalInstalls,
  SUM(mktgAttrInstalls) AS mktgAttrInstalls
FROM (
  SELECT
    FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d",
        submission_date_s3)) AS month,
    CASE
      WHEN country IN ('ca',  'fr',  'de',  'gb',  'us') THEN country
      ELSE 'non-tier1'
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
    SUM(CASE
        WHEN funnelOrigin = 'mozFunnel' THEN installs
        ELSE 0 END) AS mktgAttrInstalls
  FROM
    `ga-mozilla-org-prod-001.telemetry.corpMetrics`
  WHERE
    submission_date_s3 >= '20180101'
    AND submission_date_s3 <= '20181231'
  GROUP BY 1,2,3
  ORDER BY 1,2,5 DESC)
GROUP BY 1,2,3

UNION ALL
  (  SELECT
    FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d",
        submission_date_s3)) AS month,
    'total' AS country,
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
    SUM(CASE
        WHEN funnelOrigin = 'mozFunnel' THEN installs
        ELSE 0 END) AS mktgAttrInstalls
  FROM
    `ga-mozilla-org-prod-001.telemetry.corpMetrics`
  WHERE
    submission_date_s3 >= '20180101'
    AND submission_date_s3 <= '20181231'
  GROUP BY 1,2,3
  ORDER BY 1,2,5 DESC)