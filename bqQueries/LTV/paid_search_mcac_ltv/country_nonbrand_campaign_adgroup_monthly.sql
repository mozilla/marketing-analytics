-- Pulls together data from Fetch, GA, Corpmetrics and Telemetry to tell the
-- story of Spend, downloads, installs, LTV
with
  -- Limit Fetch data to 1) Non-branded Search in 2) top countries, 3) specific
  -- vendors, and within 4) the current quarter
  nonbranded_search_spend AS (
    SELECT
      date,
      vendor,
      adname,
      socialstring,
      country,
      targeting,
      vendornetspend,
      downloadsGA
    FROM
      `fetch.fetch_deduped`
    WHERE
      vendor IN ('Adwords', 'Bing')
      AND country IN ('United States', 'Canada', 'Germany', 'United Kingdom', 'France', 'Poland')
      AND targeting = 'Nonbrand Search'
      AND vendornetspend > 0
      AND date BETWEEN DATE(2018, 9, 1) AND DATE(2018, 12, 31)
  ),

  -- Join Fetch with LTV based on source, medium, campaign, and content
  ltv_attribution AS (
    SELECT
      PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS date_of_job,
      f.country,
      f.targeting,
      f.socialstring,
      f.vendor,
      COUNT(DISTINCT(ltv.client_ID)) AS n,
      AVG(ltv.total_clv) AS avg_tLTV
    FROM
      `ltv.v1_clients_*` AS ltv
    LEFT JOIN
      nonbranded_search_spend AS f
    ON
      ltv.content = f.adname
    WHERE
      f.targeting IS NOT NULL
      AND _TABLE_SUFFIX NOT IN ('20180608', '20180917', '20181002', '20181017')
      -- historical_searches < 5 stddevs away from mean
      AND ltv.historical_searches < (
        SELECT
          STDDEV(historical_searches) * 5
        FROM
          `ltv.v1_clients_*`
      ) + (
        SELECT
          AVG(historical_searches)
        FROM
          `ltv.v1_clients_*`
      )
    GROUP BY
      date_of_job,
      f.country,
      f.targeting,
      f.socialstring,
      f.vendor
  ),

  ltv_attribution_by_month AS (
    SELECT
      EXTRACT(MONTH FROM date_of_job) AS month_num,
      country,
      targeting,
      socialstring,
      vendor,
      MIN(n) AS min_n,
      MAX(n) AS max_n,
      AVG(avg_tLTV) AS avg_avg_tLTV
    FROM
      ltv_attribution
    GROUP BY
    month_num,
    country,
    targeting,
    socialstring,
    vendor
  )


-- Pulls whole table
SELECT
  spending.country,
  spending.targeting,
  spending.month_num,
  spending.socialstring,
  spending.vendor,
  spending.sum_vendornetspend,
  spending.sum_fetchdownloads,
  spending.proj_installs,
  spending.CPD,
  spending.proj_cpi,
  ltv_attribution_by_month.min_n,
  ltv_attribution_by_month.max_n,
  ltv_attribution_by_month.avg_avg_tLTV,
  ltv_attribution_by_month.avg_avg_tLTV * spending.proj_installs AS revenue,
  (ltv_attribution_by_month.avg_avg_tLTV * spending.proj_installs) - spending.sum_vendornetspend AS profit,
  (ltv_attribution_by_month.avg_avg_tLTV * spending.proj_installs) / spending.sum_vendornetspend AS ltv_mcac,
  CASE
    WHEN (ltv_attribution_by_month.avg_avg_tLTV * spending.proj_installs) = 0 THEN 0
    ELSE spending.sum_vendornetspend / (ltv_attribution_by_month.avg_avg_tLTV * spending.proj_installs)
  END AS mcac_ltv
FROM (
  -- group aggregations by week, country, targeting, and socialstring
  SELECT
    EXTRACT(month FROM date) AS month_num,
    f.country,
    f.targeting,
    f.socialstring,
    f.vendor,
    SUM(f.vendorNetSpend) AS sum_vendornetspend,
    SUM(f.downloadsGA) AS sum_fetchdownloads,
    SUM(f.downloadsGA) * .66 AS proj_installs,
    CASE
      WHEN SUM(f.downloadsGA) = 0 THEN 0
      ELSE SUM(f.vendornetspend) / SUM(f.downloadsGA)
    END AS CPD,
    CASE
      WHEN SUM(f.downloadsGA) = 0 THEN 0
      ELSE SUM(f.vendornetspend) / (SUM(f.downloadsGA) * .66)
    END AS proj_CPI
  FROM
    nonbranded_search_spend AS f
  GROUP BY
    month_num,
    f.country,
    f.targeting,
    f.socialstring,
    f.vendor
) AS spending
LEFT JOIN
  ltv_attribution_by_month
ON
  spending.month_num = ltv_attribution_by_month.month_num
  AND spending.country = ltv_attribution_by_month.country
  AND spending.targeting = ltv_attribution_by_month.targeting
  AND spending.socialstring = ltv_attribution_by_month.socialstring
  AND spending.vendor = ltv_attribution_by_month.vendor
ORDER BY
  spending.country,
  spending.vendor,
  spending.month_num,
  spending.socialstring

