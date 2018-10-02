SELECT
  lps.client_id,
  segment_labels,
  qualified,
  sync_configured,
  historical_searches,
  total_clv AS tLTV,
  predicted_clv_12_months AS pLTV,
  days_active,
  array_to_string(addon_names,"|")
FROM
  `ltv.screen_ltv_palomar_segments` lps
INNER JOIN qualified
  `ltv.screen_survey`
ON
  QUID = client_ID
left JOIN
  `ltv.screen_addons` aos
ON
  aos.client_id = lps.client_id