select * from (SELECT
      Created,
      AdName as AdNameTrafficking,
      TrackingLink,
      Product,
      Campaign,
      Vendor,
      Country,
      OperatingSystem,
      Device,
      Channel,
      CreativeType,
      Targeting,
      Creative,
      CreativeSize,
      CreativeConcept,
      CreativeLanguage,
      TrafficType,
      Tracking,
      Goal,
      MediaType,
      Placement,
      SocialString
FROM `ga-mozilla-org-prod-001.fetch.trafficking_*`
WHERE _TABLE_SUFFIX = (SELECT max(_table_suffix) FROM `ga-mozilla-org-prod-001.fetch.trafficking_*`)) as trafficking
LEFT JOIN
-- Join metric / spend data to campaign information data
(SELECT
*
FROM(
SELECT
  *,
  MAX(ingestionDate) OVER (PARTITION BY date, Adname) AS IngestionDateMax
FROM (
  SELECT
    _table_suffix AS ingestionDate,
    *
  FROM
    `ga-mozilla-org-prod-001.fetch.metric_*`))
-- Filter for the most recent metric update
WHERE ingestionDate = IngestionDateMax) as metric
ON trafficking.AdNameTrafficking = metric.Adname