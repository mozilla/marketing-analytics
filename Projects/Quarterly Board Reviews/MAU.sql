-- https://sql.telemetry.mozilla.org/queries/1703/source#3027

WITH sample AS
(
   SELECT *
   FROM client_count_daily
   WHERE submission_date > '20170600'
),
dau AS
(
  SELECT country, submission_date, merge(cast(hll AS HLL)) AS hll FROM sample
  GROUP BY submission_date, country
  HAVING cardinality(merge(cast(hll AS HLL))) > 25 -- remove outliers
  ORDER BY country, submission_date DESC
),
smoothed_dau AS
(
  SELECT country, submission_date, avg(cardinality(hll)) OVER (PARTITION BY country ORDER BY submission_date ROWS BETWEEN 6 PRECEDING AND 0 FOLLOWING) AS count
  FROM dau
),
mau AS
(
  SELECT country, submission_date, merge(hll) OVER (PARTITION BY country ORDER BY submission_date ROWS BETWEEN 27 PRECEDING AND 0 FOLLOWING) AS hll
  FROM dau
),
countries AS
(
  SELECT country, cardinality(merge(cast(hll as HLL))) as hll
  FROM sample
  GROUP BY country
  ORDER BY 2 DESC
  LIMIT 100
)
SELECT dau.country,
       date_format(date_parse(dau.submission_date, '%Y%m%d'), '%Y-%m-%d') AS activity_date,
       cardinality(mau.hll) AS MAU,
       cardinality(dau.hll) AS DAU,
       smoothed_dau.count AS SMOOTH_DAU,
       smoothed_dau.count/cardinality(mau.hll) AS ER
FROM mau, dau, smoothed_dau, countries
WHERE mau.country = dau.country AND
      mau.submission_date = dau.submission_date AND
      dau.country = smoothed_dau.country AND
      dau.submission_date = smoothed_dau.submission_date AND
      dau.country = countries.country