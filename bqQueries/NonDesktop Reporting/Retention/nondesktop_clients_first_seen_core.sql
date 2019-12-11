WITH data AS (
           -- Fields that would be of interest for acquisition reporting
          SELECT
            submission_date,
            metadata.document_id,
            metadata.timestamp,
            client_id,
            app_name,
            metadata_app_version,
            os,
            osversion,
            metadata.normalized_channel,
            metadata.geo_country AS country,
            metadata.geo_city AS city,
            locale,
            '' as device_manufacturer,
            device,
            arch,
            metadata.app_build_id,
            distribution_id,
            campaign,
            campaign_id,
            profile_date
          FROM
            `moz-fx-data-derived-datasets.telemetry.telemetry_core_parquet_v3`
          WHERE
            submission_date = "{submission_date_filter}"),

        -- Order daily records for each client
         identify_daily_duplicates AS (
          SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY client_id, submission_date ORDER BY timestamp) AS record_count
          FROM data),

        -- Find first record submitted for each client daily
        deduplicate_daily AS(
          SELECT
            * EXCEPT (record_count)
          FROM
            identify_daily_duplicates
          WHERE
            record_count = 1),

        first_seen as(
        SELECT
          client_id,
          first_seen_date
        FROM
          `moz-fx-data-derived-datasets.analysis.nondesktop_clients_first_seen`
        WHERE
            app_name != 'Fenix'
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

        -- Select new clients to append to the non_desktop_first_seen table
        SELECT
            submission_date as first_seen_date,
            document_id,
            timestamp,
            client_id,
            NULL as sample_id,
            app_name,
            metadata_app_version as app_version,
            os,
            osversion as os_version,
            normalized_channel,
            country,
            city,
            locale,
            device_manufacturer,
            device as device_model,
            arch as architecture,
            app_build_id as app_build,
            distribution_id,
            campaign,
            campaign_id,
            profile_date as profile_creation_date
        FROM
          new_clients
        WHERE
          first_seen_join_date IS NULL