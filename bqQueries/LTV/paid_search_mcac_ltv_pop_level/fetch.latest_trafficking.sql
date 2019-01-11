SELECT
  *
FROM
  `ga-mozilla-org-prod-001.fetch.trafficking_*`
WHERE
  _TABLE_SUFFIX = (SELECT MAX(_TABLE_SUFFIX) FROM `ga-mozilla-org-prod-001.fetch.trafficking_*`)
