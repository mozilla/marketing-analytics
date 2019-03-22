

with mobileData as (
SELECT
    client_id,
    submission_date_s3
FROM telemetry_core_parquet_v3
WHERE
    app_name in ('Zerda')
    AND submission_date_s3 >= '20161201'
GROUP BY
    client_id,
    submission_date_s3
),

clients AS (
SELECT
    date(date_parse(submission_date_s3, '%Y%m%d')) AS submissionDateS3,
    date(date_add('day',27,date_parse(submission_date_s3,'%Y%m%d'))) AS mauUntil,
    client_id as clientID
 FROM mobileData),

dates AS (
SELECT
    DISTINCT submissionDateS3
FROM clients
)

SELECT
    dates.submissionDateS3,
    COUNT(DISTINCT clients.clientID) AS mau
FROM dates
JOIN
    clients
ON
    dates.submissionDateS3 BETWEEN clients.submissionDateS3 and clients.mauUntil
GROUP BY
    dates.submissionDateS3
ORDER BY dates.submissionDateS3
