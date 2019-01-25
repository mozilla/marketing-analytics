SELECT
  submission_date_s3,
  SUM(CASE WHEN funnelOrigin = 'darkFunnel' THEN installs ELSE 0 END) AS darkFunnel_installs,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' THEN installs ELSE 0 END) AS mozFunnel_installs,
  SUM(installs) AS total_installs,
  SUM(CASE WHEN funnelOrigin = 'darkFunnel' THEN DAU ELSE 0 END) AS darkFunnel_DAU,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' THEN DAU ELSE 0 END) AS mozFunnel_DAU,
  SUM(DAU) AS total_DAU,
  SUM(CASE WHEN funnelOrigin = 'darkFunnel' THEN activeDAU ELSE 0 END) AS darkFunnel_aDAU,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' THEN activeDAU ELSE 0 END) AS mozFunnel_aDAU,
  SUM(activeDAU) AS total_aDAU
FROM
  `ga-mozilla-org-prod-001.telemetry.corpMetrics`
WHERE submission_date_s3 >= '20180101'
GROUP BY 1
ORDER BY 1