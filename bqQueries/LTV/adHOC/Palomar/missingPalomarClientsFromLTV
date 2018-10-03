with palomar as
(select client_id p_client_id, country_limited, os_limited, has_update_auto_download, has_telemetry_enabled, default_browser, last_default_search_engine, app_version_trunc,  used_tracking_protection,
count(submission_date_s3) as days_dau,
sum(is_adau) as days_active,
sum(is_adau)/count(submission_date_s3) pct_days_active,
sum(if(is_weekday=true,is_adau,0)) as active_weekdays,
sum(if(is_weekday=false,is_adau,0)) as active_weekend_days,
max(num_addons) max_addons,
avg(daily_searches) avg_daily_searches,
sum(daily_searches) sum_daily_searches,
sum(if(is_weekday=true,daily_searches,0)) as sum_weekday_searches,
sum(if(is_weekday=false,daily_searches,0)) as sum_weekend_searches,
avg(daily_uri_count) avg_daily_uri_count,
sum(daily_uri_count) sum_daily_uri_count,
sum(if(is_weekday=true,daily_uri_count,0)) as sum_weekday_uri_count,
sum(if(is_weekday=false,daily_uri_count,0)) as sum_weekend_uri_count,
avg(daily_active_hours) as avg_daily_active_hours,
sum(daily_active_hours) as sum_daily_active_hours,
sum(if(is_weekday=true,daily_active_hours,0)) as sum_weekday_active_hours,
sum(if(is_weekday=false,daily_active_hours,0)) as sum_weekend_active_hours,
avg(daily_usage_hours) as avg_daily_usage_hours,
sum(daily_usage_hours) as sum_daily_usage_hours,
sum(if(is_weekday=true,daily_usage_hours,0)) as sum_weekday_usage_hours,
sum(if(is_weekday=false,daily_usage_hours,0)) as sum_weekend_usage_hours,
max(submission_date_s3) most_recent_day,
max(if(is_adau=1,submission_date_s3,null)) as most_recent_active_day
From `ltv.palomar`
Group by
1,2,3,4,5,6,7,8,9
),

value as (
select * from `ltv.ltv_v1`
)

select count(p_client_Id) p_clients,
Sum(If(client_id is null, 1,0)) missing_p_clients,
Sum(If(client_id is null, 1,0))/count(p_client_Id) pct_missing_ltv_clients

from palomar
left join value on palomar.p_client_id = value.client_id
 where most_recent_active_day > 20180710
