-- Pulls together data from Fetch, GA, Corpmetrics and Telemetry to tell the
-- story of Spend, downloads, installs, LTV
WITH
  -- sum of dau and installs by ad content
  dau_installs_by_content AS (
    SELECT
      PARSE_DATE('%Y%m%d', submission_date_s3) as date,
      REPLACE(REPLACE(REPLACE(REPLACE(contentCleaned,'%2528','('),'%2529',')'),'%2B',' '),'%257C','|') AS content,
      SUM(installs) AS sum_installs,
      SUM(dau) AS sum_dau
    FROM
      `telemetry.corpMetrics`
    GROUP BY
      date,
      content
  ),

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
    AND targeting = 'Nonbrand Search'
    AND vendornetspend > 0
    AND date BETWEEN DATE(2018, 9, 1) AND DATE(2018, 12, 31)
  ),

  -- Joins with LTV data set based on source, medium, campaign, and content
  ltv_attribution AS (
  SELECT
    f.country,
    f.targeting,
    -- NOTE: this is only used for what seems to be an optional grouping
    -- CASE
    --   WHEN REGEXP_EXTRACT(f.socialstring, r'(.*)_') LIKE '%Competitor%' THEN 'Competitor'
    --   WHEN REGEXP_EXTRACT(f.socialstring, r'(.*)_') LIKE '%Browser%' THEN 'Browser'
    -- END AS AGroup,
    COUNT(DISTINCT(ltv.client_ID)) AS n,
    AVG(ltv.total_clv) AS avg_tLTV
  FROM
    `ltv.v1_clients_20181017` AS ltv
  LEFT JOIN
    `fetch.fetch_deduped` AS f
  ON
    ltv.content = f.adname
  WHERE
    ltv.historical_searches < (
      SELECT
        STDDEV(historical_searches) * 5
      FROM
        `ltv.v1_clients_20181017`
    ) + (
      SELECT
        AVG(historical_searches)
      FROM
        `ltv.v1_clients_20181017`
    )
  GROUP BY
    f.country,
    f.targeting
    -- AGroup
  )

-- Pulls whole table
SELECT
  spending.country,
  spending.targeting,
  spending.week_num,
  spending.sum_vendornetspend,
  spending.sum_fetchdownloads,
  spending.proj_installs,
  spending.CPD,
  spending.proj_cpi,
  ltv_attribution.n,
  ltv_attribution.avg_tLTV,
  ltv_attribution.avg_tLTV * spending.proj_installs AS revenue,
  (ltv_attribution.avg_tLTV * spending.proj_installs) - spending.sum_vendornetspend AS profit,
  (ltv_attribution.avg_tLTV * spending.proj_installs) / spending.sum_vendornetspend AS ltv_mcac,
  CASE
    WHEN (ltv_attribution.avg_tLTV * spending.proj_installs) = 0 THEN 0
    ELSE spending.sum_vendornetspend / (ltv_attribution.avg_tLTV * spending.proj_installs)
  END AS mcac_ltv
FROM (
  SELECT
    EXTRACT(week FROM f.date) AS week_num,
    f.country,
    f.targeting,

    SUM(vendorNetSpend) AS sum_vendornetspend,
    -- sum(downloads) sum_downloads,
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
  LEFT JOIN
    -- NOTE: nothing is projected from this table at the moment
    dau_installs_by_content AS t
  ON
    f.date = t.date
    AND f.adname = t.content
  GROUP BY
    week_num,
    f.country,
    f.targeting
) AS spending
LEFT JOIN
  ltv_attribution
ON
  spending.country = ltv_attribution.country
  AND spending.targeting = ltv_attribution.targeting
ORDER BY
  spending.country,
  spending.week_num,
  spending.sum_vendornetspend ASC
