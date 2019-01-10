SELECT
submission_date_s3 AS submissionDate,
funnelOrigin AS funnelOrigin,
country, 
sum(DAU) as DAU,
sum(activeDAU) as aDAU,
sum(installs) as installs,
sum(searches) as searches
FROM
`ga-mozilla-org-prod-001.telemetry.corpMetrics`
WHERE country in ('us', 'ca', 'de', 'fr', 'uk', 'gb')
 AND funnelOrigin = "mozFunnel"
GROUP By 1,2,3
