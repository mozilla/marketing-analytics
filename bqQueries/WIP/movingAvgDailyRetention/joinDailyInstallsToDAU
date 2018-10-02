WITH
  metrics AS(
  SELECT
    retained.*,
    installs.installs
  FROM ( (
      SELECT
        *
      FROM
        `ga-mozilla-org-prod-001.telemetry.dailyRetentionv1`) AS retained
    LEFT JOIN (
      SELECT
        *
      FROM
        `ga-mozilla-org-prod-001.telemetry.installsTemp`) AS installs
    ON
      retained.installDate = installs.installDate ))
SELECT
  *,
  CASE WHEN daysRetained<0 OR daysRetained IS NULL AND submission_date_s3 LIKE '2017%' AND installDate = '19700101' THEN 'existing' ELSE 'new2018' END as acqSegmentCleaned,
  CASE WHEN installs IS NOT NULL AND daysRetained>=0 THEN DAU/installs else 0 END as retention
FROM
  metrics