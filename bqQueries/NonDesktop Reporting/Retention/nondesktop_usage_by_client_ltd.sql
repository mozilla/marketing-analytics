WITH lastSeenData AS (
  SELECT
    submission_date,
    client_id,
    days_since_seen,
    app_name,
    SPLIT(metadata_app_version,'.')[offset(0)] as app_version,
    os,
    normalized_channel,
    campaign,
    country,
    distribution_id
  FROM
    `moz-fx-data-shared-prod.telemetry.core_clients_last_seen`
  WHERE submission_date = "{submission_date_filter}"

  UNION ALL

  SELECT
    submission_date,
    client_id,
    days_since_seen,
    app_name,
    SPLIT(app_display_version,'.')[offset(0)] as app_version,
    os,
    normalized_channel,
    NULL AS campaign,
    country,
    NULL AS distribution_id
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix.clients_last_seen`
  WHERE
    submission_date = "{submission_date_filter}"
),

acquisitionData as(
SELECT
  client_id,
  first_seen_date
FROM
  `moz-fx-data-derived-datasets.analysis.nondesktop_clients_first_seen`
GROUP BY
  1,2),

countryNames as(
SELECT
      UPPER(rawCountry) as country_code,
      standardizedCountry as country_name
    FROM
      `moz-fx-data-derived-datasets.analysis.standardized_country_list`)

SELECT
  submission_date,
  acquisitionData.first_seen_date,
  CASE app_name
    WHEN 'Fennec' THEN CONCAT(app_name, ' ', os)
    WHEN 'Focus' THEN CONCAT(app_name, ' ', os)
    WHEN 'Lockbox' THEN CONCAT('Lockwise ', os)
    WHEN 'Zerda' THEN 'Firefox Lite'
    ELSE app_name
  END AS product_name,
  os,
  country,
  countryNames.country_name,
  app_version,
  COUNTIF(days_since_seen < 1) AS dau,
  COUNTIF(days_since_seen < 7) AS wau,
  COUNTIF(days_since_seen < 28) AS mau
FROM
  lastSeenData
LEFT JOIN
  acquisitionData
ON
  lastSeenData.client_id = acquisitionData.client_id
LEFT JOIN
  countryNames
ON
  lastSeenData.country = countryNames.country_code
WHERE
  app_name IN (
    'Fenix',
    'Fennec', -- Firefox for Android and Firefox for iOS
    'Focus',
    'Lockbox', -- Lockwise
    'Zerda', -- Firefox Lite, previously called Rocket
    'FirefoxForFireTV', -- Amazon Fire TV
    'FirefoxConnect') -- Amazon Echo Show
  AND os IN ('Android', 'iOS')
  -- 2017-01-01 is the first populated day of telemetry_core_parquet, so start 28 days later.
  AND submission_date >= DATE '2017-01-28'
GROUP BY 1,2,3,4,5,6,7