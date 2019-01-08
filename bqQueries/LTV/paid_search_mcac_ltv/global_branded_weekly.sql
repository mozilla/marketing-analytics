-- Pulls together data from Fetch, GA, and Telemetry to tell the story of ad
-- spend, downloads, installs, and LTV
WITH
  -- Limit Fetch data to 1) Non-branded Search in 2) top countries, 3) specific
  -- vendors, and within 4) the current quarter
  nonbranded_search_spend AS (
    SELECT
      date,
      vendor,
      adname,
      country,
      targeting,
      vendornetspend,
      downloadsGA
    FROM
      `fetch.fetch_deduped`
    WHERE
      vendor IN ('Adwords', 'Bing')
      AND country IN ('United States', 'Canada', 'Germany')
      AND targeting = 'Brand Search'
      AND vendornetspend > 0
      AND date BETWEEN DATE(2018, 7, 1) AND DATE(2018, 12, 31)
  ),

  -- Join Fetch with LTV based on source, medium, campaign, and content
  ltv_attribution AS (
    SELECT
      EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', _TABLE_SUFFIX)) AS week_num,
      f.targeting,
      COUNT(DISTINCT(ltv.client_ID)) AS n,
      AVG(ltv.total_clv) AS avg_tLTV
    FROM
      `ltv.v1_clients_*` AS ltv
    -- NOTE: this join might be deferrable until later?
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
      week_num,
      f.targeting
  )

-- Pulls whole table
SELECT
  spending.targeting,
  spending.week_num,
  spending.sum_vendornetspend,
  spending.sum_fetchdownloads,
  spending.estimated_installs,
  spending.cpd,
  spending.estimated_cpi,
  ltv_attribution.n,
  ltv_attribution.avg_tLTV,
  ltv_attribution.avg_tLTV * spending.estimated_installs AS estimated_revenue,
  (ltv_attribution.avg_tLTV * spending.estimated_installs) - spending.sum_vendornetspend AS net_cost_of_acquisition,
  (ltv_attribution.avg_tLTV * spending.estimated_installs) / spending.sum_vendornetspend AS ltv_mcac,
  CASE
    WHEN (ltv_attribution.avg_tLTV * spending.estimated_installs) = 0 THEN 0
    ELSE spending.sum_vendornetspend / (ltv_attribution.avg_tLTV * spending.estimated_installs)
  END AS mcac_ltv
FROM (
  SELECT
    EXTRACT(week FROM f.date) AS week_num,
    f.targeting,

    SUM(f.vendorNetSpend) AS sum_vendornetspend,
    --  sum(downloads) sum_downloads,
    SUM(f.downloadsGA) AS sum_fetchdownloads,
    SUM(f.downloadsGA) * .66 AS estimated_installs,
    CASE
      WHEN SUM(f.downloadsGA) = 0 THEN 0
      ELSE SUM(f.vendornetspend) / SUM(f.downloadsGA)
    END AS estimated_cpd,
    CASE
      WHEN SUM(f.downloadsGA) = 0 THEN 0
      ELSE SUM(f.vendornetspend) / (SUM(f.downloadsGA) * .66)
    END AS estimated_cpi
  FROM
    nonbranded_search_spend AS f
  GROUP BY
    week_num,
    f.targeting
) AS spending
LEFT JOIN
  ltv_attribution
ON
  spending.week_num = ltv_attribution.week_num
  AND spending.targeting = ltv_attribution.targeting
ORDER BY
  spending.week_num,
  spending.sum_vendornetspend ASC
