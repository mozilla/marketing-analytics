-- Used to check whether the detail fetch spend is in line with overall fetch spend in the fetch system (i.e. Summary file)

WITH comparison as(
SELECT
fetchSummary.month,
fetchSummary.year,
fetchSummary.fetchSummarySpend,
detailSummary.detailVendorNetSpend,
fetchSummary.fetchSummarySpend - detailSummary.detailVendorNetSpend as variance

FROM(
(SELECT
month,
year,
ROUND(sum(vendorNetSpend),0) as fetchSummarySpend
FROM `ga-mozilla-org-prod-001.fetch.summary_20180821`
GROUP BY 1,2
ORDER BY 2,1) as fetchSummary

LEFT JOIN

(SELECT
  EXTRACT(MONTH FROM date) AS month,
  EXTRACT(YEAR FROM date) AS year,
  ROUND(SUM(vendorNetSpend)) AS detailVendorNetSpend
FROM
  `ga-mozilla-org-prod-001.fetch.fetch_deduped`
GROUP BY
  1,  2
ORDER BY
  2,  1) as detailSummary

ON fetchSummary.month = detailSummary.month AND fetchSummary.year=detailSummary.year))

SELECT * From comparison
ORDER By 2, 1