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
FROM `ga-mozilla-org-prod-001.fetch.trafficking`) as trafficking
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
WHERE ingestionDate = IngestionDateMax) as metric
ON trafficking.AdNameTrafficking = metric.Adname