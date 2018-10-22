with
  active_clients AS (
    SELECT
      submission_date_s3,
      country,
      client_id,
      predicted_clv_12_months,
      max_activity_date,
      profile_creation_date,
      profile_age_in_days,
      CASE
        WHEN DATE_DIFF(max_activity_date, profile_creation_date, MONTH) = 0 THEN 0
        ELSE days_active / DATE_DIFF(max_activity_date, profile_creation_date, MONTH)
      END AS days_active_per_month
    FROM
      `ltv.screen_ltv_palomar`
    WHERE
      -- ailve and active within the last 28 days
      user_status = 'Active'
      AND activity_group = 'aDAU'
      AND DATE_DIFF(DATE(2018,9,16), max_activity_date, DAY) < 28
      -- in US, Canada, and Germany
      AND country IN ('US', 'CA', 'DE')
      -- with a valid profile creation date
      AND profile_creation_date IS NOT NULL
      AND profile_creation_date != DATE(1970, 1, 1)
      -- with profile age outliers removed
      AND profile_age_in_days > 0
      AND profile_age_in_days < (
        SELECT
          STDDEV(profile_age_in_days) * 2.5
        FROM
          `ltv.screen_ltv_palomar`
      ) + (
        SELECT
          AVG(profile_age_in_days)
        FROM
          `ltv.screen_ltv_palomar`
      )
  ),

  client_z_scores AS (
SELECT
  client_id,

  (avg_predicted_clv_12_months - predicted_clv_12_months) / stddev_predicted_clv_12_months AS z_predicted_clv_12_months,
  (avg_days_active_per_month - days_active_per_month) / stddev_days_active_per_month AS z_days_active_per_month,
  ((avg_predicted_clv_12_months - predicted_clv_12_months) / stddev_predicted_clv_12_months +
   (avg_days_active_per_month - days_active_per_month) / stddev_days_active_per_month) / 2 AS avg_z_score
FROM
  active_clients
CROSS JOIN (
  SELECT
    AVG(predicted_clv_12_months) AS avg_predicted_clv_12_months,
    STDDEV(predicted_clv_12_months) AS stddev_predicted_clv_12_months,
    AVG(days_active_per_month) AS avg_days_active_per_month,
    STDDEV(days_active_per_month) AS stddev_days_active_per_month
  FROM
    active_clients
)
)

SELECT
  active_clients.country,

  COUNT(active_clients.client_id) * 100 AS n,
  approx_quantiles(client_z_scores.avg_z_score, 20)[offset(15)] AS q80_avg_z_score,
  approx_quantiles(client_z_scores.avg_z_score, 20)[offset(17)] AS q90_avg_z_score,
  approx_quantiles(client_z_scores.avg_z_score, 20)[offset(18)] AS q95_avg_z_score,
  SUM(active_clients.predicted_clv_12_months) * 100 AS sum_predicted_ltv_12_months
FROM
  active_clients
LEFT JOIN
  client_z_scores
ON
  active_clients.client_id = client_z_scores.client_id
GROUP BY
  active_clients.country
ORDER BY
  active_clients.country

-- hacky template for by-country & per-quantile aggregations
/*
SELECT
  COUNT(client_id) * 100 AS n,
  ROUND(SUM(predicted_clv_12_months), 2) * 100 AS sum_predicted_ltv_12_months,
  ROUND(SUM(predicted_clv_12_months) * 100 / 28049676 * 100, 1) AS pct_revenue
FROM
  active_clients
WHERE
  country = 'DE'
  AND days_active_per_month >= 26.0
*/
