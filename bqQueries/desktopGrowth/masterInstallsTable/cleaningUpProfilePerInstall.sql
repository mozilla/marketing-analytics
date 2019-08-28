WITH newProfiles as (SELECT
  submission,
  client_id,
  CASE WHEN metadata.geo_country IS NULL THEN '' ELSE metadata.geo_country END as country,
  CASE WHEN environment.settings.attribution.source IS NULL THEN 'unknown' ELSE environment.settings.attribution.source END as source,
  CASE WHEN environment.settings.attribution.medium IS NULL THEN 'unknown' ELSE environment.settings.attribution.medium END as medium,
  CASE WHEN environment.settings.attribution.campaign IS NULL THEN 'unknown' ELSE environment.settings.attribution.campaign END as campaign,
  CASE WHEN environment.settings.attribution.content IS NULL THEN 'unknown' ELSE environment.settings.attribution.content END as content,
  CASE WHEN environment.partner.distribution_id IS NULL THEN '' ELSE environment.partner.distribution_id END as distribution_id,
  CASE
    WHEN environment.settings.attribution.source IS NULL AND environment.settings.attribution.medium IS NULL
        AND environment.settings.attribution.campaign IS NULL AND environment.settings.attribution.content IS NULL
        AND environment.partner.distribution_id IS NULL THEN 'darkFunnel'
    ELSE CASE
    WHEN environment.settings.attribution.source IS NULL AND environment.settings.attribution.medium IS NULL
        AND environment.settings.attribution.campaign IS NULL AND environment.settings.attribution.content IS NULL
        AND environment.partner.distribution_id IS NOT NULL THEN 'partnerships'
    ELSE 'mozFunnel'
  END END AS funnelOrigin
FROM
  `moz-fx-data-derived-datasets.telemetry.telemetry_new_profile_parquet_v2`
WHERE
  submission = "2019-05-23"
GROUP BY 1,2,3,4,5,6,7,8,9),

firstShutdown as (
SELECT
  submission_date_s3,
  'firstShutdown' as tableOrigin,
  client_id as fs_client_id,
  scalar_parent_startup_profile_selection_reason as profileSelectionReason
FROM
  `moz-fx-data-derived-datasets.telemetry.first_shutdown_summary_v4`
WHERE
  submission_date_s3 >= "2019-05-23"
  AND submission_date_s3 <= "2019-05-24"
GROUP BY 1,2,3,4
),

mainSummary as (
SELECT
  submission_date_s3,
  'mainSummary' as tableOrigin,
  client_id as ms_client_id,
  scalar_parent_startup_profile_selection_reason as profileSelectionReason
FROM
  `moz-fx-data-derived-datasets.telemetry.main_summary_v4`
WHERE
  submission_date_s3 >= "2019-05-23"
  AND submission_date_s3 <= "2019-05-24"
GROUP BY 1,2,3,4
),

mainPingsJoin as (
SELECT
  submission_date_s3,
  tableOrigin,
  fs_client_id,
  CASE
    WHEN profileSelectionReason IN ('restart-skipped-default', 'firstrun-skipped-default') THEN CONCAT('01-',profileSelectionReason) ELSE
    CASE WHEN profileSelectionReason IS NULL THEN 'zz-null' ELSE profileSelectionReason
    END END as profileSelectionReason
FROM
  firstShutDown

UNION ALL
SELECT
  submission_date_s3,
  tableOrigin,
  ms_client_id,
  CASE
    WHEN profileSelectionReason IN ('restart-skipped-default', 'firstrun-skipped-default') THEN CONCAT('01-',profileSelectionReason) ELSE
    CASE WHEN profileSelectionReason IS NULL THEN 'zz-null' ELSE profileSelectionReason
    END END as profileSelectionReason
FROM
  mainSummary
),

firstOccurenceMainPing as (
SELECT
  submission_date_s3,
  tableOrigin,
  fs_client_id as joinClientID,
  profileSelectionReason,
  firstShow
FROM (
  SELECT
    submission_date_s3,
    tableOrigin,
    fs_client_id,
    profileSelectionReason,
    ROW_NUMBER() OVER(PARTITION BY fs_client_id ORDER BY submission_date_s3, profileSelectionReason, tableOrigin) as firstShow
  FROM
    mainPingsJoin)
WHERE firstShow = 1
),

joined as (
SELECT
  *
FROM
  newProfiles
LEFT JOIN
  firstOccurenceMainPing
ON
  newProfiles.client_id = firstOccurenceMainPing.joinClientID)

SELECT
      submission,
      client_id,
      country,
      source,
      medium,
      campaign,
      content,
      distribution_id,
      funnelOrigin,
      tableOrigin,
      profileSelectionReason
FROM joined
WHERE profileSelectionReason NOT LIKE '%-skipped-default'
