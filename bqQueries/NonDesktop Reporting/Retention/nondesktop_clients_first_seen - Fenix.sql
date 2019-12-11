WITH data AS (
   -- Fields that would be of interest for acquisition reporting
  SELECT
    submission_date,
    client_id,
    sample_id,
    app_name,
    app_display_version as app_version,
    country,
    city,
    locale,
    device_manufacturer,
    device_model,
    architecture,
    os,
    os_version,
    normalized_channel,
    app_build,
    UNIX_DATE(first_run_date) as first_run_date
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_derived.clients_daily_v1`
  WHERE
  -- For initial table setup, ran from the beginning of the dataset. Daily updates look at previous day
    submission_date = "{submission_date_filter}"),

-- Order daily records for each client
identify_client_duplicates AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY submission_date) AS record_count
  FROM
    data),

-- Find first record submitted for each client during the time window
deduplicate_daily AS(
  SELECT
    * EXCEPT (record_count)
  FROM
    identify_client_duplicates
  WHERE
    record_count = 1),

first_seen as(
SELECT
  client_id,
  first_seen_date
FROM
  `moz-fx-data-derived-datasets.analysis.nondesktop_clients_first_seen`
WHERE
    app_name = 'Fenix'
GROUP BY 1,2),

-- Check against clients in non_desktop_first_seen table to see new clients
new_clients as(
SELECT
  deduplicate_daily.*,
  first_seen.first_seen_date as first_seen_join_date
FROM
  deduplicate_daily
LEFT JOIN
  first_seen as first_seen
ON
  deduplicate_daily.client_id = first_seen.client_id)


-- Select the clients not found in the first_seen table and these are the ones that will be inserted into the table
SELECT
  submission_date as first_seen_date,
  '' as document_id,
  NULL as timestamp,
  client_id,
  sample_id,
  app_name,
  app_version,
  os,
  os_version,
  normalized_channel,
  country,
  city,
  locale,
  device_manufacturer,
  device_model,
  architecture,
  app_build,
  first_run_date as profile_creation_date
FROM
  new_clients
WHERE
    first_seen_join_date IS NULL