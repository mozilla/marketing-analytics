WITH
  value AS (
  SELECT
    client_id,
    REPLACE(REPLACE(REPLACE(REPLACE(campaign,'%2528','('),'%2529',')'),'%2B',' '),'%257C','|') t_campaign,
    total_clv
  FROM
    `ltv.ltv_v1`
  WHERE
    historical_searches < (
    SELECT
      STDDEV(historical_searches)
    FROM
      `ltv.ltv_v1`) *2.5 + (
    SELECT
      AVG(historical_searches)
    FROM
      `ltv.ltv_v1`)
      ),

    campaigns as (
    SELECT
      distinct campaign,
      channel,
      type,
      market
     From
     `ltv.campaigns_20180808`)

 Select market,type, count(distinct(client_id)) n, avg(value.total_clv) avg_tLTV
 From
 value
 inner join campaigns on value.t_campaign = campaigns.campaign
 Group by 1,2
 order by 1 desc, 2 desc