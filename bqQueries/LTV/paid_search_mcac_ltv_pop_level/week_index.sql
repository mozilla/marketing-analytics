SELECT
  date,
  extract(week from date) as week_num
FROM
  unnest(generate_date_array(date(2019, 1, 1), date(2019, 12, 31), interval 1 week)) as date
