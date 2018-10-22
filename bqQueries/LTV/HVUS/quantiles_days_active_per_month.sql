WITH
  active_clients AS (
    SELECT
      clients.submission_date_s3,
      clients.country,
      clients.client_id,
      clients.predicted_clv_12_months,
      clients.max_activity_date,
      clients.profile_creation_date,
      clients.profile_age_in_days,
      additional_user_metrics.days_active,
      CASE
        WHEN DATE_DIFF(parse_DATE('%Y-%m-%d',  max_activity_date), parse_DATE('%Y-%m-%d',  profile_creation_date), MONTH) = 0 THEN 0
        ELSE days_active / DATE_DIFF(parse_DATE('%Y-%m-%d',
          max_activity_date), parse_DATE('%Y-%m-%d',
          profile_creation_date), MONTH)
      END AS days_active_per_month,
      additional_user_metrics.* EXCEPT (client_id)
    FROM
      `ltv.v1_clients_20181011` AS clients
    RIGHT JOIN
      `ltv.v1_additional_user_metrics` additional_user_metrics
    ON
      clients.client_id = additional_user_metrics.client_id
    WHERE
      -- active within the last 28 days
      -- user_status = 'Active'
      -- AND activity_group = 'aDAU'
      -- AND DATE_DIFF(DATE(2018, 10, 8), parse_date('%Y-%m-%d', max_activity_date), DAY) < 28
      DATE_DIFF(DATE(2018, 10, 8), parse_date('%Y-%m-%d', submission_date_s3), DAY) <= 28
      AND DATE_DIFF(DATE(2018, 10, 8), parse_date('%Y%m%d', cast(max_submission_date_s3 as string)), DAY) <= 28
      -- in US, Canada, and Germany
      AND country IN ('US', 'CA', 'DE')
      -- with predicted_clv_12_months outliers removed
      AND predicted_clv_12_months < (
        SELECT
          STDDEV(predicted_clv_12_months) * 2.5
        FROM
          `ltv.v1_clients_20181011`
      ) + (
        SELECT
          AVG(predicted_clv_12_months)
        FROM
          `ltv.v1_clients_20181011`
      )
      -- with a valid profile creation date
      AND profile_creation_date IS NOT NULL
      AND parse_DATE('%Y-%m-%d', profile_creation_date) != DATE(1970, 1, 1)
      -- with profile age outliers removed
      AND profile_age_in_days > 0
      AND profile_age_in_days < (
        SELECT
          STDDEV(profile_age_in_days) * 2.5
        FROM
          `ltv.v1_clients_20181011`
      ) + (
        SELECT
          AVG(profile_age_in_days)
        FROM
          `ltv.v1_clients_20181011`
      )
  ),

  pull_delta AS (
    SELECT n_ltv / n_palomar AS correction
    FROM
      (SELECT COUNT(*) AS n_ltv FROM `ltv.v1_clients_20181011`)
    CROSS JOIN
      (SELECT COUNT(*) n_palomar FROM `ltv.v1_additional_user_metrics`)
  ),

  quantiles AS (
    SELECT
      country,
      ROUND(COUNT(client_id) * (SELECT correction FROM pull_delta) * 100, 0) AS num_clients,
      ROUND(approx_quantiles(days_active_per_month, 20)[offset(14)], 2) AS q75_days_active_per_month,
      ROUND(approx_quantiles(days_active_per_month, 20)[offset(15)], 2) AS q80_days_active_per_month,
      ROUND(approx_quantiles(days_active_per_month, 20)[offset(17)], 2) AS q90_days_active_per_month,
      ROUND(approx_quantiles(days_active_per_month, 20)[offset(18)], 2) AS q95_days_active_per_month,
      ROUND(SUM(predicted_clv_12_months) * (SELECT correction FROM pull_delta) * 100, 0) AS sum_predicted_ltv_12_months,
      AVG(days_active_per_month) AS avg_days_active_per_month,
      SUM(days_active_per_month) AS sum_days_active_per_month
    FROM
      active_clients
    GROUP BY
      country
    ORDER BY
      country
  )

-- SELECT * FROM quantiles

SELECT
  quantiles.country,
  ROUND(COUNT(active_clients.client_id) * (SELECT correction FROM pull_delta) * 100, 2) AS num_clients,
  ROUND(SUM(active_clients.predicted_clv_12_months) * (SELECT correction FROM pull_delta) * 100, 2) AS sum_pLTV,
  AVG(active_clients.days_active_per_month) AS avg_days_active_per_month,
  SUM(active_clients.days_active_per_month) AS sum_days_active_per_month
FROM
  quantiles
LEFT JOIN
  active_clients
ON
  quantiles.country = active_clients.country
WHERE
  active_clients.days_active_per_month >= quantiles.q75_days_active_per_month
GROUP BY
  country
ORDER BY
  country
