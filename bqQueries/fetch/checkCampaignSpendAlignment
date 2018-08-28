-- Used to check whether the detail fetch spend is in line with overall fetch spend in the fetch system (i.e. Summary file)

WITH comparison as(
SELECT
fetchSummary.campaign,
fetchSummary.fetchSummarySpend,
detailSummary.detailVendorNetSpend,
fetchSummary.fetchSummarySpend - detailSummary.detailVendorNetSpend as variance

FROM(
(SELECT
campaign,
ROUND(sum(vendorNetSpend),0) as fetchSummarySpend
FROM `ga-mozilla-org-prod-001.fetch.summary_20180821`
GROUP BY 1
ORDER BY 2 DESC) as fetchSummary

LEFT JOIN

(SELECT
  campaign AS campaign,
  ROUND(SUM(vendorNetSpend)) AS detailVendorNetSpend
FROM
  `ga-mozilla-org-prod-001.fetch.fetch_deduped`
GROUP BY
  1
ORDER BY
  2 DESC) as detailSummary

ON fetchSummary.campaign = detailSummary.campaign))

SELECT * From comparison
ORDER By 2 DESC