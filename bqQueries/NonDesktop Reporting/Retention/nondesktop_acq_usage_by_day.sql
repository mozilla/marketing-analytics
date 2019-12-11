WITH cohortData as (
    SELECT
      *
    FROM
      `moz-fx-data-derived-datasets.analysis.nondesktop_clients_first_seen`
    WHERE
      app_name IN ("Fennec", "Focus", "Zerda", "FirefoxConnect")
      AND os IN ("Android")
      AND first_seen_date <= "{submission_date_filter}"),

    dauData as (
    SELECT
      *
    FROM
      `moz-fx-data-derived-datasets.telemetry.core_clients_last_seen_v1`
    WHERE
      app_name IN ("Fennec", "Focus", "Zerda", "FirefoxConnect")
      AND os IN ("Android")
      AND days_since_seen < 1
      AND submission_date = "{submission_date_filter}"),

    countryNames as(
    SELECT
      UPPER(rawCountry) as country_code,
      standardizedCountry as country_name
    FROM
      `moz-fx-data-derived-datasets.analysis.standardized_country_list`),

    joinedData as(
    SELECT
      cohortData.* EXCEPT (document_id, timestamp),
      dauData.submission_date,
      dauData.app_name as current_app_name,
      dauData.os as current_os,
      dauData.osversion as current_os_version,
      dauData.metadata_app_version as current_app_version,
      dauData.normalized_channel as current_normalized_channel,
      dauData.locale as current_locale,
      dauData.country as current_country,
      currentCountryNames.country_name as current_country_name,
      CAST(NULL as string) as current_device_manufacturer,
      dauData.device as current_device_model,
      countryNames.country_name as country_name
    FROM
      cohortData
    LEFT JOIN
      dauData
    ON
      cohortData.client_id = dauData.client_id
    LEFT JOIN
      countryNames
    ON
      cohortData.country = countryNames.country_code
    LEFT JOIN
      countryNames as currentCountryNames
    ON
      dauData.country = currentCountryNames.country_code)

    SELECT
      first_seen_date,
      CASE app_name
        WHEN 'Fennec' THEN CONCAT(app_name, ' ', os)
        WHEN 'Focus' THEN CONCAT(app_name, ' ', os)
        WHEN 'Lockbox' THEN CONCAT('Lockwise ', os)
        WHEN 'Zerda' THEN 'Firefox Lite'
        ELSE app_name
      END AS product_name,
      app_name,
      app_version,
      os,
      os_version,
      normalized_channel,
      locale,
      country,
      country_name,
      device_manufacturer,
      device_model,
      current_app_name,
      current_app_version,
      current_os,
      current_os_version,
      current_normalized_channel,
      current_locale,
      current_country,
      current_country_name,
      current_device_manufacturer,
      current_device_model,
      submission_date,
      COUNT(DISTINCT client_id) as client_count
    FROM
      joinedData
    GROUP BY
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23