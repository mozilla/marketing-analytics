WITH parameters as(
SELECT
  PARSE_DATE('%Y-%m-%d', '2018-01-01') as startDate,
  PARSE_DATE('%Y-%m-%d', '2019-03-06') as endDate),

usageData as(
SELECT
  submission_date as submission,
  CASE WHEN country IS NULL THEN '' ELSE country END as country,
  CASE WHEN source IS NULL THEN '' ELSE source END as source,
  CASE WHEN medium IS NULL THEN '' ELSE medium END as medium,
  CASE WHEN campaign IS NULL THEN '' ELSE campaign END as campaign,
  CASE WHEN content IS NULL THEN '' ELSE content END as content,
  CASE WHEN distribution_id IS NULL THEN '' ELSE distribution_id END as distribution_id,
  SUM(dau) as dau,
  SUM(mau) as mau
FROM `moz-fx-data-derived-datasets.analysis.firefox_desktop_exact_mau28_by_dimensions_v1`
WHERE
  submission_date >= (SELECT parameters.startDate from parameters)
  AND submission_date <= (SELECT parameters.endDate from parameters)
GROUP BY
  submission,
  country,
  source,
  medium,
  campaign,
  content,
  distribution_id),

installData as (
SELECT
  submission,
  CASE WHEN metadata.geo_country IS NULL THEN '' ELSE metadata.geo_country END as country,
  CASE WHEN environment.settings.attribution.source IS NULL THEN '' ELSE environment.settings.attribution.source END as source,
  CASE WHEN environment.settings.attribution.medium IS NULL THEN '' ELSE environment.settings.attribution.medium END as medium,
  CASE WHEN environment.settings.attribution.campaign IS NULL THEN '' ELSE environment.settings.attribution.campaign END as campaign,
  CASE WHEN environment.settings.attribution.content IS NULL THEN '' ELSE environment.settings.attribution.content END as content,
  CASE WHEN environment.partner.distribution_id IS NULL THEN '' ELSE environment.partner.distribution_id END as distribution_id,
  COUNT(DISTINCT client_id) as installs
FROM
  `moz-fx-data-derived-datasets.telemetry.telemetry_new_profile_parquet_v2`
WHERE
  submission >= (SELECT parameters.startDate FROM parameters)
  AND submission <= (SELECT parameters.endDate FROM parameters)
GROUP BY
  submission,
  country,
  source,
  medium,
  campaign,
  content,
  distribution_id),

desktopKeyMetrics as (
SELECT
  usageData.submission,
  usageData.country,
  usageData.source,
  usageData.medium,
  usageData.campaign,
  usageData.content,
  usageData.distribution_id,
  usageData.dau,
  usageData.mau,
  installData.installs
FROM
  usageData
FULL JOIN
  installData
ON
  usageData.submission = installData.submission
  AND usageData.country = installData.country
  AND usageData.source = installData.source
  AND usageData.medium = installData.medium
  AND usageData.campaign = installData.campaign
  AND usageData.content = installData.content
  AND usageData.distribution_id = installData.distribution_id)

SELECT * FROM desktopKeyMetrics