WITH dailyDeduped as(
SELECT
submission_date_s3,
client_id,
country,
attribution.source,
attribution.medium,
attribution.campaign,
attribution.content,
SUM(scalar_parent_browser_engagement_total_uri_count_sum) as totalURI
FROM
telemetry.clients_daily_v6
WHERE
/*submission_date_s3 > DATE_ADD('2018-12-15', INTERVAL -28 DAY)
AND */submission_date_s3 /*<*/= '2018-12-15'
GROUP BY
submission_date_s3,
client_id,
country,
attribution.source,
attribution.medium,
attribution.campaign,
attribution.content
),

deduped as (SELECT *,
MAX(totalURI) OVER(PARTITION BY submission_date_s3, client_id, country, source, medium, campaign, content) as maxURI
FROM dailyDeduped)


SELECT * FROM deduped WHERE totalURI != maxURI
