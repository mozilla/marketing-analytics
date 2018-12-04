-- Pull for Xuan for email before download experiment

SELECT
  date AS date,
  SUM(CASE WHEN content LIKE "downloader_email_form_experiment_va" THEN IF(downloads > 0,1,0) ELSE 0 END) AS downloadsVerA,
  SUM(CASE WHEN content LIKE "downloader_email_form_experiment_vb" THEN IF(downloads > 0,1,0) ELSE 0 END) AS downloadsVerB,
  SUM(CASE WHEN content LIKE "downloader_email_form_experiment_vc" THEN IF(downloads > 0,1,0) ELSE 0 END) AS downloadsVerC
FROM (
  SELECT
    date AS date,
    fullVisitorId AS visitorId,
    visitNumber AS visitNumber,
    device.browser AS browser,
    trafficSource.adContent AS content,
    SUM(IF (hits.eventInfo.eventAction = "Firefox Download",
        1,
        0)) AS downloads
  FROM
    `ga-mozilla-org-prod-001.65789850.ga_sessions_*`,
    UNNEST (hits) AS hits
  WHERE
    _TABLE_SUFFIX >= '20180901'
    AND hits.type = 'EVENT'
    AND hits.eventInfo.eventCategory IS NOT NULL
    AND hits.eventInfo.eventLabel LIKE "Firefox for Desktop%"
    AND trafficSource.adContent LIKE "downloader_email_form%"
  GROUP BY
    1,
    2,
    3,
    4,
    5)
GROUP BY
  1
ORDER BY
  1