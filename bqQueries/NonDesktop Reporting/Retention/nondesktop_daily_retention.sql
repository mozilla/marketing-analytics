WITH cohort as (
    SELECT
      first_seen_date,
      product_name,
      os,
      -- Can't join on NULLs
      CASE WHEN country IS NULL THEN '' ELSE country END as country,
      CASE WHEN normalized_channel IS NULL THEN '' ELSE normalized_channel END as normalized_channel,
      SUM(client_count) as cohort
    FROM
      `moz-fx-data-derived-datasets.analysis.nondesktop_acq_usage_by_day`
    WHERE
      (submission_date = first_seen_date)
      AND first_seen_date <= "{submission_date_filter}"
    GROUP BY
      1,2,3,4,5),

    usage as(
    SELECT
      submission_date,
      first_seen_date,
      product_name,
      os,
      CASE WHEN country IS NULL THEN '' ELSE country END as country,
      CASE WHEN normalized_channel IS NULL THEN '' ELSE normalized_channel END as normalized_channel,
      SUM(client_count) as users
    FROM
      `moz-fx-data-derived-datasets.analysis.nondesktop_acq_usage_by_day`
    WHERE
      submission_date = "{submission_date_filter}"
    GROUP BY
      1,2,3,4,5,6),

    countryNames as(
    SELECT
      UPPER(rawCountry) as country_code,
      standardizedCountry as country_name
    FROM
      `moz-fx-data-derived-datasets.analysis.standardized_country_list`)

    SELECT
      cohort.first_seen_date,
      cohort.product_name,
      cohort.os,
      cohort.country,
      countryNames.country_name,
      cohort.normalized_channel,
      SUM(cohort.cohort) as cohort,
      usage.submission_date,
      DATE_DIFF(submission_date, cohort.first_seen_date, DAY) as days_retained,
      SUM(users) as users
    FROM
      cohort
    LEFT JOIN
      usage
    ON
      cohort.first_seen_date = usage.first_seen_date
      AND cohort.product_name = usage.product_name
      AND cohort.os = usage.os
      AND cohort.country = usage.country
      AND cohort.normalized_channel = usage.normalized_channel
    LEFT JOIN
      countryNames
    ON
      cohort.country = countryNames.country_code
    GROUP BY
      1,2,3,4,5,6,8