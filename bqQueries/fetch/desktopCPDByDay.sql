-- Calculate the cpd of

SELECT
  date,
  SUM(vendorNetSpend) AS vendorNetSpend,
  SUM(DownloadsGA) AS gaDownloads,
  CASE WHEN SUM(DownloadsGA) = 0 THEN NULL ELSE sum(DownloadsGA)/SUM(VendorNetSpend) END as CPD
FROM
  `ga-mozilla-org-prod-001.fetch.fetch_deduped`
WHERE
  device IN ('Desktop','MultiChannel')
  AND GOAL = 'Performance'
GROUP BY 1
ORDER BY 1