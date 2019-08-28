-- Query used to pull data from new profiles table to join to first shutdown and main summary pulls

SELECT
  submission,
  'newProfile' as tableOrigin,
  client_id as client_id,
  metadata.geo_country as country,
  metadata.geo_city as city,
  metadata.geo_subdivision1 as geo_subdivision1,
  metadata.geo_subdivision2 as geo_subdivision2,
  metadata.normalized_channel as channel,
  SPLIT(environment.build.version,'.')[offset (0)] as buildVersion,
  environment.partner.distribution_id as distribution_id,
  environment.system.os.name as os,
  environment.settings.attribution.source,
  environment.settings.attribution.medium,
  environment.settings.attribution.campaign,
  environment.settings.attribution.content
FROM
  `moz-fx-data-derived-datasets.telemetry.telemetry_new_profile_parquet_v2`
WHERE
  submission = "2019-06-24"
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15