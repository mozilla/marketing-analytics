SELECT
  submission_date_s3,
  CASE
    WHEN country IN ('CA',  'FR',  'DE',  'GB',  'US') THEN country
    ELSE 'non-tier1'
  END AS country,
  SUM(aDAU) AS totalaDAU,
  SUM(CASE WHEN funnel_origin = 'mozFunnel' THEN aDAU ELSE 0 END) as mktgAttraDAU,
  SUM(CASE WHEN funnel_origin = 'mozFunnel' AND acq_segment = 'existing' THEN aDAU ELSE 0 END) as existingaDAU,
  SUM(CASE WHEN funnel_origin = 'mozFunnel' AND acq_segment = 'new2018' AND FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', install_date)) = '201801' THEN aDAU ELSE 0 END) as JanAcq,
  SUM(CASE WHEN funnel_origin = 'mozFunnel' AND acq_segment = 'new2018' AND FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', install_date)) = '201802' THEN aDAU ELSE 0 END) as FebAcq,
  SUM(CASE WHEN funnel_origin = 'mozFunnel' AND acq_segment = 'new2018' AND FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', install_date)) = '201803' THEN aDAU ELSE 0 END) as MarAcq,
  SUM(CASE WHEN funnel_origin = 'mozFunnel' AND acq_segment = 'new2018' AND FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', install_date)) = '201804' THEN aDAU ELSE 0 END) as AprAcq,
  SUM(CASE WHEN funnel_origin = 'mozFunnel' AND acq_segment = 'new2018' AND FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', install_date)) = '201805' THEN aDAU ELSE 0 END) as MayAcq,
  SUM(CASE WHEN funnel_origin = 'mozFunnel' AND acq_segment = 'new2018' AND FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', install_date)) = '201806' THEN aDAU ELSE 0 END) as JunAcq,
  SUM(CASE WHEN funnel_origin = 'mozFunnel' AND acq_segment = 'new2018' AND FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', install_date)) = '201807' THEN aDAU ELSE 0 END) as JulAcq,
  SUM(CASE WHEN funnel_origin = 'mozFunnel' AND acq_segment = 'new2018' AND FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', install_date)) = '201808' THEN aDAU ELSE 0 END) as AugAcq,
  SUM(CASE WHEN funnel_origin = 'mozFunnel' AND acq_segment = 'new2018' AND FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', install_date)) = '201809' THEN aDAU ELSE 0 END) as SepAcq,
  SUM(CASE WHEN funnel_origin = 'mozFunnel' AND acq_segment = 'new2018' AND FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', install_date)) = '201810' THEN aDAU ELSE 0 END) as OctAcq,
  SUM(CASE WHEN funnel_origin = 'mozFunnel' AND acq_segment = 'new2018' AND FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', install_date)) = '201811' THEN aDAU ELSE 0 END) as NovAcq,
  SUM(CASE WHEN funnel_origin = 'mozFunnel' AND acq_segment = 'new2018' AND FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', install_date)) = '201812' THEN aDAU ELSE 0 END) as DecAcq
FROM
  `ga-mozilla-org-prod-001.telemetry.dailyRetentionV2`
WHERE
submission_date_s3 >= '20181001'
GROUP BY
  1,2
ORDER BY
  2,1