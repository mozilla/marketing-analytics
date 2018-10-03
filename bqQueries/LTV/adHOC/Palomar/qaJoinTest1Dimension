WITH
  palomar AS (
  SELECT
    client_id p_client_id,
--    country_limited,
--    os_limited,
--    has_update_auto_download,
--    has_telemetry_enabled,
--    default_browser,
--    last_default_search_engine,
--    app_version_trunc,
--    used_tracking_protection,
    COUNT(submission_date_s3) AS days_dau,
    SUM(is_adau) AS days_active,
    SUM(is_adau)/COUNT(submission_date_s3) pct_days_active,
    SUM(IF(is_weekday=TRUE,
        is_adau,
        0)) AS active_weekdays,
    SUM(IF(is_weekday=FALSE,
        is_adau,
        0)) AS active_weekend_days,
    MAX(num_addons) max_addons,
    AVG(daily_searches) avg_daily_searches,
    SUM(daily_searches) sum_daily_searches,
    SUM(IF(is_weekday=TRUE,
        daily_searches,
        0)) AS sum_weekday_searches,
    SUM(IF(is_weekday=FALSE,
        daily_searches,
        0)) AS sum_weekend_searches,
    AVG(daily_uri_count) avg_daily_uri_count,
    SUM(daily_uri_count) sum_daily_uri_count,
    SUM(IF(is_weekday=TRUE,
        daily_uri_count,
        0)) AS sum_weekday_uri_count,
    SUM(IF(is_weekday=FALSE,
        daily_uri_count,
        0)) AS sum_weekend_uri_count,
    AVG(daily_active_hours) AS avg_daily_active_hours,
    SUM(daily_active_hours) AS sum_daily_active_hours,
    SUM(IF(is_weekday=TRUE,
        daily_active_hours,
        0)) AS sum_weekday_active_hours,
    SUM(IF(is_weekday=FALSE,
        daily_active_hours,
        0)) AS sum_weekend_active_hours,
    AVG(daily_usage_hours) AS avg_daily_usage_hours,
    SUM(daily_usage_hours) AS sum_daily_usage_hours,
    SUM(IF(is_weekday=TRUE,
        daily_usage_hours,
        0)) AS sum_weekday_usage_hours,
    SUM(IF(is_weekday=FALSE,
        daily_usage_hours,
        0)) AS sum_weekend_usage_hours,
    CASE
      WHEN MIN(submission_date_s3) < 20161101 THEN 20000101
      ELSE MIN(submission_date_s3)
    END AS birth_date,
    CASE
      WHEN MAX(submission_date_s3) < 20161101 THEN 20000101
      ELSE MAX(submission_date_s3)
    END AS recent_date
  FROM
    `ltv.palomar`
  GROUP BY
    1
--    2,
--    3,
--    4,
--    5,
--    6,
--    7,
--    8,
--    9
  having recent_date > 20180710
  and birth_Date > 20000101
    ),
  value AS (
  SELECT
    *
  FROM
    `ltv.ltv_v1_20180807`
  WHERE
    historical_searches < (
    SELECT
      STDDEV(historical_searches)
    FROM
      `ltv.ltv_v1_20180807`) *2.5 + (
    SELECT
      AVG(historical_searches)
    FROM
      `ltv.ltv_v1_20180807`))
SELECT
  *
FROM
  value
INNER JOIN
  palomar
ON
  value.client_id = palomar.p_client_id