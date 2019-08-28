-- Query used to pull data from first shutdown table to join to new profile and main summary pulls

SELECT
  submission_date_s3,
  'firstShutdown' as tableOrigin,
  client_id,
  country,
  city,
  geo_subdivision1,
  geo_subdivision2,
  normalized_channel as channel,
  SPLIT(app_version,'.')[offset (0)] as buildVersion,
  distribution_id,
  os,
  attribution.source,
  attribution.medium,
  attribution.campaign,
  attribution.content,
  null as profileSelectionReason
FROM
  `moz-fx-data-derived-datasets.telemetry.first_shutdown_summary_v4`
WHERE
  submission_date_s3 = "2019-06-23"
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16