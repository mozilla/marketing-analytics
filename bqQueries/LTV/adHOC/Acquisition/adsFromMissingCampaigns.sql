select
socialstring,
  Targeting,
  vendor,
  country,
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