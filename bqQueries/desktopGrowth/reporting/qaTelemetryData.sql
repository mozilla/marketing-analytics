

-- DAU clients daily
SELECT
  COUNT(DISTINCT client_id) as DAU
FROM
  `moz-fx-data-derived-datasets.telemetry.clients_daily_v6`
WHERE
  submission_date_s3 = DATE("2019-02-20")


-- MAU clients daily
SELECT
  COUNT(DISTINCT client_id) as MAU
FROM
  `moz-fx-data-derived-datasets.telemetry.clients_daily_v6`
WHERE
  submission_date_s3 >= DATE("2019-01-24") AND submission_date_s3 <= DATE("2019-02-20")


-- MAU main summary
SELECT
  COUNT(DISTINCT client_id) as MAU
FROM
  `moz-fx-data-derived-datasets.telemetry.main_summary_v4`
WHERE
  submission_date_s3 >= DATE("2019-01-24") AND submission_date_s3 <= DATE("2019-02-20")


-- DAU main summary
SELECT
  COUNT(DISTINCT client_id) as DAU
FROM
  `moz-fx-data-derived-datasets.telemetry.main_summary_v4`
WHERE
  submission_date_s3 = DATE("2019-02-20")