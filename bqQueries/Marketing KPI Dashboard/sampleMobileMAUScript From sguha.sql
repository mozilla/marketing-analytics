with a as (
select
client_id,
submission_date_s3,
count(*) as n0
from telemetry.telemetry_core_parquet_v3
where app_name in ('Fennec', 'Focus', 'Zerda', 'FirefoxForFireTV', 'FirefoxConnect')
and submission_date_s3 >= '2017-01-01'
group by 1,2
),
clients AS (
 SELECT
  submission_date_s3 AS d,
  date_add(submission_date_s3, INTERVAL 27 DAY) AS mau_until,
  client_id as cid
 FROM
  a
), dates AS (
 SELECT
  DISTINCT d
 FROM
  clients
 WHERE
  d >= '2017-01-01'
)

SELECT
 dates.d,
 COUNT(DISTINCT clients.cid) AS mau
FROM
 dates
JOIN
 clients
ON
 dates.d BETWEEN clients.d and clients.mau_until
GROUP BY
 1
order by 1