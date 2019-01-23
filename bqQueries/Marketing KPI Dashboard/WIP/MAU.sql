-- Sample script from Jesse Mccrosky in the Fx Metrics channel Jan 4th 2019

SELECT
  dates_table.submission_date_s3,
  COUNT(DISTINCT clients_table.client_id) AS mau
FROM
  (SELECT TO_DATE(submission_date_s3, 'yyyyMMdd') AS submission_date_s3 FROM clients_daily_v6 WHERE submission_date_s3 >= '20181216' GROUP BY submission_date_s3) dates_table
JOIN
  (SELECT client_id, TO_DATE(submission_date_s3, 'yyyyMMdd') AS submission_date_s3 FROM clients_daily_v6 WHERE submission_date_s3 >= '20181119') clients_table
ON
  clients_table.submission_date_s3 between dates_table.submission_date_s3 - interval 27 day and dates_table.submission_date_s3
GROUP BY dates_table.submission_date_s3