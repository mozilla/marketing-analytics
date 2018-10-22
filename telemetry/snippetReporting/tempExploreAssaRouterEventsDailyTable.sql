-- Used to pull data by client_id to load into pyspark for comparison to mainsummary aDAU / DAU Pulls

SELECT
    date,
    impression_id as client_id,
    release_channel,
    SUM(CASE WHEN event = 'IMPRESSION' THEN 1 ELSE 0 END) as impressions,
    SUM(CASE WHEN event = 'CLICK_BUTTON' THEN 1 ELSE 0 END) as clicks,
    SUM(CASE WHEN event = 'BLOCK' THEN 1 ELSE 0 END) as blocks
FROM assa_router_events_daily
WHERE
    source = 'snippets_user_event'
    AND date = '2018-10-15'
GROUP BY 1,2,3
HAVING impressions > 9
ORDER BY 4 DESC
