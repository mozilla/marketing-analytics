SELECT
  performance.*,
  metaData.name,
  metaData.campaign,
  metaData.category,
  metaData.url,
  metaData.body
FROM
  `ga-mozilla-org-prod-001.snippets.snippets_performance_*` as performance
LEFT JOIN
  `ga-mozilla-org-prod-001.snippets.snippets_metadata` as metaData
ON
performance.snippetID = metaData.id