-- Snippet analytics tracking migrates from GA to telemetry in early 2019
-- This query runs on stmo and is scheduled to run daily at 6:00am
-- Provides a high level summary of performance by snippet message ID

SELECT
    date,
    message_id,
    release_channel,
    locale,
    country_code,
    os,
    version,
    SUM(CASE WHEN event = 'IMPRESSION' THEN 1 ELSE 0 END) as impressions,
    SUM(CASE WHEN event = 'CLICK_BUTTON' THEN 1 ELSE 0 END) as clicks,
    SUM(CASE WHEN event = 'BLOCK' THEN 1 ELSE 0 END) as blocks
FROM assa_router_events_daily
WHERE
    source = 'snippets_user_event'
    AND date = current_date-1
GROUP BY 1,2,3,4,5,6,7
ORDER By 8 DESC
