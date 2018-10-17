SELECT
    date,
    impression_id as client_id,
    os,
    version,
    release_channel,
    SUM(CASE WHEN event = 'IMPRESSION' THEN 1 ELSE 0 END) as impressions,
    SUM(CASE WHEN event = 'CLICK_BUTTON' THEN 1 ELSE 0 END) as clicks,
    SUM(CASE WHEN event = 'BLOCK' THEN 1 ELSE 0 END) as blocks
FROM assa_router_events_daily
WHERE
    source = 'snippets_user_event'
    AND date = '2018-10-15'
GROUP BY 1,2,3,4,5
ORDER BY 6 DESC
