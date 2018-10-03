With corp as(
Select * from `telemetry.corpMetrics`
Where submission_date_s3 >= '20180101'
),

camp as (
select 
socialstring,
  Targeting,
  vendor,
  country as c_country, 
  Adname
from `fetch.fetch_deduped`
Where placement like 'Search'
And date > '2018-07-01'
and (SocialString like 'Firefox|BN|Search|NB|Exact|SP|US|EN|DK|Text|Competitor%'
OR SocialString like 'Firefox|GG|Search|NB|BMM|SP|DE|TS|DK|Text|Browser%'
OR SocialString like 'Firefox|BN|Search|NB|Exact|SP|DE|TS|DK|Text|Competitor%'
OR SocialString like 'Firefox|GG|Search|NB|Exact|SP|US|EN|DK|Text|Competitor%'
OR SocialString like 'Firefox|GG|Search|NB|Broad|SP|US|EN|DK|Text|Browser%'
OR SocialString like 'Firefox|GG|Search|NB|BMM|SP|DE|TS|DK|Text|Competitor%'
OR SocialString like 'Firefox|GG|Search|NB|Exact|SP|DE|TS|DK|Text|Competitor%'
OR SocialString like 'Firefox|BN|Search|NB|Exact|SP|CA|EN|DK|Text|Competitor%'
OR SocialString like 'Firefox|GG|Search|NB|BMM|SP|US|EN|DK|Text|Competitor%'
OR SocialString like 'Firefox|BN|Search|NB|Exact|SP|DE|EN|DK|Text|Competitor%'
OR SocialString like 'Firefox|GG|Search|NB|Exact|SP|US|EN|DK|Text|Browser%'
OR SocialString like 'Firefox|GG|Search|NB|Exact|SP|DE|TS|DK|Text|Browser%'
OR SocialString like 'Firefox|BN|Search|NB|BMM|SP|DE|TS|DK|Text|Competitor%'
OR SocialString like 'Firefox|GG|Search|NB|Exact|SP|CA|EN|DK|Text|Competitor%'
OR SocialString like 'Firefox|BN|Search|NB|BMM|SP|DE|EN|DK|Text|Competitor%'
OR SocialString like 'Firefox|GG|Search|NB|Broad|SP|CA|EN|DK|Text|Browser%'
OR SocialString like 'Firefox|BN|Search|NB|BMM|SP|CA|EN|DK|Text|Competitor%'
OR SocialString like 'Firefox|GG|Search|NB|Broad|SP|DE|EN|DK|Text|Browser%'
OR SocialString like 'Firefox|GG|Search|NB|BMM|SP|CA|EN|DK|Text|Competitor%'
OR SocialString like 'Firefox|GG|Search|NB|Broad|SP|US|EN|DK|Text|Competitor%'
OR SocialString like 'Firefox|BN|Search|NB|BMM|SP|CA|EN|DK|Text|Browser%'
OR SocialString like 'Firefox|GG|Search|NB|BMM|SP|DE|EN|DK|Text|Competitor%'
OR SocialString like 'Firefox|GG|Search|NB|Exact|SP|DE|EN|DK|Text|Competitor%'
OR SocialString like 'Firefox|GG|Search|NB|Broad|SP|CA|EN|DK|Text|Competitor%'
OR SocialString like 'Firefox|GG|Search|NB|Broad|SP|DE|EN|DK|Text|Competitor%'
OR SocialString like 'Firefox|GG|Search|NB|Exact|SP|CA|EN|DK|Text|Browser%'
OR SocialString like 'Firefox|GG|Search|NB|Exact|SP|DE|EN|DK|Text|Browser%'
OR SocialString like 'Firefox|BN|Search|NB|BMM|SP|DE|EN|DK|Text|Browser%'
OR SocialString like 'Brand-PL-EN-GGL-BMM%'
OR SocialString like 'Firefox|GG|Search|NB|Exact|SP|US|EN|DK|Text|Web Trackers%'
OR SocialString like 'Firefox|GG|Search|NB|BMM|SP|US|EN|DK|Text|Web Trackers%')
),

value as (
select
client_id,
source, 
medium, 
campaign,
country,
content,
total_clv as tLTV
from `ltv.ltv_v1`
)

select socialstring, value.campaign, targeting, vendor, value.country, count(distinct(client_id)) n,  avg(tLTV) avg_tLTV
from value
inner join camp on value.content = camp.AdName

--left join value on corp.content = value.content
Group by 1,2,3,4,5
Order by 6 desc