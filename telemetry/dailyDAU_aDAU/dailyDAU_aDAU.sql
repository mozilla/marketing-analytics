with a as ( select
submission_date_s3 as date,
count(*) AS adau
FROM clients_daily
WHERE
    scalar_parent_browser_engagement_total_uri_count_sum >= 5
    AND submission_date_s3 >= '20160101'
GROUP BY 1
ORDER BY 1)
, b as (select
submission_date_s3 as date,
count(*) AS dau
FROM clients_daily
WHERE submission_date_s3 >= '20160101'
GROUP BY 1
ORDER BY 1)

select a.date, a.adau, b.dau
from a join b
on a.date=b.date
order by 1


