--Marketing Attributable Clients by country
--Includes traffic that belongs to a known marketing attributable medium (cpc, paidsearch, email, snippet, video, native, display, social
--Excludes outliers where variance > 2.5 standard deviations from the mean


SELECT
 -- REPLACE(REPLACE(REPLACE(REPLACE(source,'%2528','('),'%2529',')'),'%2B',' '),'%257C','|') cSource,
 -- REPLACE(REPLACE(REPLACE(REPLACE(medium,'%2528','('),'%2529',')'),'%2B',' '),'%257C','|') cMedium,
 -- REPLACE(REPLACE(REPLACE(REPLACE(campaign,'%2528','('),'%2529',')'),'%2B',' '),'%257C','|') cCampaign,
 -- REPLACE(REPLACE(REPLACE(REPLACE(content,'%2528','('),'%2529',')'),'%2B',' '),'%257C','|') cContent,
  count(distinct(client_ID)) n,
  avg(total_clv) avg_tLTV
  
FROM
  `ltv.ltv_v1`
WHERE
-- Exclude outliers
 historical_searches < (
  SELECT
    STDDEV(historical_searches)
  FROM
    `ltv.ltv_v1`) *2.5 + (
  SELECT
    AVG(historical_searches)
  FROM
    `ltv.ltv_v1`)
  AND (content like  ('%acebook%')
  OR campaign like ('%acebook%')
  OR source like ('%acebook%')
  OR medium like ('%acebook%')
  OR content like  ('%witter%')
  OR campaign like ('%witter%')
  OR source like ('%witter%')
  OR medium like ('%witter%'))
  AND Campaign not like ('unsupported-browser-notification')
--  Group by 1,2,3,4
  Order by n desc