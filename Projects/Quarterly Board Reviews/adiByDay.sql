-- https://sql.telemetry.mozilla.org/queries/59575/source#154092

SELECT bl_date AS date,
          SUM(tot_requests_on_date) AS adi
   FROM copy_adi_dimensional_by_date
   WHERE product = 'Firefox'
     AND bl_date >= '2014-01-01'
     GROUP BY 1
   ORDER BY 1