SELECT
  date,
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
  SUM(totalaDAU) AS totalaDAU,
  SUM(mktgAttraDAU) AS mktgAttraDAU
FROM (
  SELECT
    submission_date_s3 as date,
    CASE
      WHEN country IN ('ca',  'fr',  'de',  'gb',  'us') THEN country
      ELSE 'non-tier1'
    END AS country,
    SUM(activeDAU) AS totalaDAU,
    SUM(CASE
        WHEN funnelOrigin = 'mozFunnel' THEN activeDAU
        ELSE 0 END) AS mktgAttraDAU
  FROM
    `ga-mozilla-org-prod-001.telemetry.corpMetrics`
  WHERE
    submission_date_s3 >= '20180101'
    AND submission_date_s3 <= '20181231'
  GROUP BY 1,2
  ORDER BY 2,1)
GROUP BY 1,2

UNION ALL
  (  SELECT
     submission_date_s3 as date,
    'total' AS country,
    SUM(activeDAU) AS totalaDAU,
    SUM(CASE
        WHEN funnelOrigin = 'mozFunnel' THEN activeDAU
        ELSE 0 END) AS mktgAttraDAU
  FROM
    `ga-mozilla-org-prod-001.telemetry.corpMetrics`
  WHERE
    submission_date_s3 >= '20180101'
    AND submission_date_s3 <= '20181231'
  GROUP BY 2,1
  ORDER BY 1,2,4 DESC)
ORDER BY 2,1