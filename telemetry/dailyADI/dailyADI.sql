-- This is for the daily ADI data 


-- query link: https://sql.telemetry.mozilla.org/queries/59575/source for ADI Data (runs once every 24hrs)
-- You need to contact Jason Thomas for access to vertica data source (the actual query in telemetry listed below)

SELECT bl_date AS date,
          SUM(tot_requests_on_date) AS adi
   FROM copy_adi_dimensional_by_date
   WHERE product = 'Firefox'
     AND bl_date >= '2014-01-01' 
     GROUP BY 1
   ORDER BY 1

-- You have access to the results stored in csv file via following link
-- https://sql.telemetry.mozilla.org/api/queries/59575/results.csv?api_key=0M9ISHsK2U8ZrwqHp6EzobHWISRhahlZB94DojZa 
