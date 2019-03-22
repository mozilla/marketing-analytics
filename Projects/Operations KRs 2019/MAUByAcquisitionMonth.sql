SELECT
  submission_date,
  CASE
    WHEN country IN ('CA',  'FR',  'DE',  'GB',  'US') THEN country
    ELSE 'non-tier1'
  END AS country,
  funnelOrigin,
  SUM(MAU) AS totalMAU,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' THEN MAU ELSE 0 END) as mktgAttrMAU,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' AND acqSegment = 'existing' THEN MAU ELSE 0 END) as existingMAU,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' AND acqSegment = 'new2018' AND FORMAT_DATE('%Y%m', installDate) = '201801' THEN MAU ELSE 0 END) as JanAcq,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' AND acqSegment = 'new2018' AND FORMAT_DATE('%Y%m', installDate) = '201802' THEN MAU ELSE 0 END) as FebAcq,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' AND acqSegment = 'new2018' AND FORMAT_DATE('%Y%m', installDate) = '201803' THEN MAU ELSE 0 END) as MarAcq,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' AND acqSegment = 'new2018' AND FORMAT_DATE('%Y%m', installDate) = '201804' THEN MAU ELSE 0 END) as AprAcq,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' AND acqSegment = 'new2018' AND FORMAT_DATE('%Y%m', installDate) = '201805' THEN MAU ELSE 0 END) as MayAcq,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' AND acqSegment = 'new2018' AND FORMAT_DATE('%Y%m', installDate) = '201806' THEN MAU ELSE 0 END) as JunAcq,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' AND acqSegment = 'new2018' AND FORMAT_DATE('%Y%m', installDate) = '201807' THEN MAU ELSE 0 END) as JulAcq,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' AND acqSegment = 'new2018' AND FORMAT_DATE('%Y%m', installDate) = '201808' THEN MAU ELSE 0 END) as AugAcq,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' AND acqSegment = 'new2018' AND FORMAT_DATE('%Y%m', installDate) = '201809' THEN MAU ELSE 0 END) as SepAcq,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' AND acqSegment = 'new2018' AND FORMAT_DATE('%Y%m', installDate) = '201810' THEN MAU ELSE 0 END) as OctAcq,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' AND acqSegment = 'new2018' AND FORMAT_DATE('%Y%m', installDate) = '201811' THEN MAU ELSE 0 END) as NovAcq,
  SUM(CASE WHEN funnelOrigin = 'mozFunnel' AND acqSegment = 'new2018' AND FORMAT_DATE('%Y%m', installDate) = '201812' THEN MAU ELSE 0 END) as DecAcq
FROM
  `ga-mozilla-org-prod-001.telemetry.dailyRetentionv3MAU`
WHERE
submission_date = '2018-12-15'
GROUP BY
  1,2,3
ORDER BY
  2,3