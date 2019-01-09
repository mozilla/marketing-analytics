-- https://sql.telemetry.mozilla.org/queries/60849/source
 WITH sample AS
  ( SELECT *
   FROM client_count_daily
   WHERE submission_date > '20170600' ),

      dau AS
  ( SELECT submission_date,
           merge(cast(hll AS HLL)) AS hll
   FROM sample
   GROUP BY submission_date
   HAVING cardinality(merge(cast(hll AS HLL))) > 25 -- remove outliers
   ORDER BY submission_date DESC),

      smoothed_dau AS
  ( SELECT submission_date,
           avg(cardinality(hll)) OVER (
                                       ORDER BY submission_date ROWS BETWEEN 6 PRECEDING AND 0 FOLLOWING) AS COUNT
   FROM dau),

      mau AS
  ( SELECT submission_date,
           merge(hll) OVER (
                            ORDER BY submission_date ROWS BETWEEN 27 PRECEDING AND 0 FOLLOWING) AS hll
   FROM dau)

SELECT date_format(date_parse(dau.submission_date, '%Y%m%d'), '%Y-%m-%d') AS activity_date,
       cardinality(mau.hll) AS MAU,
       cardinality(dau.hll) AS DAU,
       smoothed_dau.count AS SMOOTH_DAU,
       smoothed_dau.count/cardinality(mau.hll) AS ER
FROM mau,
     dau,
     smoothed_dau
WHERE mau.submission_date = dau.submission_date
  AND dau.submission_date = smoothed_dau.submission_date
ORDER BY activity_date ASC