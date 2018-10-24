with
  attributed_clients as (
    select
      *
    from
      `ltv.v1_clients_20181011`
    where
      country is not null
      -- clients attributed to moz.org
      AND attribution_site = 'www_mozilla_org'
      -- with outliers removed by historical searches
      AND historical_searches < (
        SELECT
          STDDEV(historical_searches) * 5
        FROM
          `ltv.v1_clients_20181011`
      ) + (
        SELECT
          AVG(historical_searches)
        FROM
          `ltv.v1_clients_20181011`
      )
  ),

  clients as (
    select
      country,
      count(*) as n,
      avg(total_clv) as LTV,
      sum(predicted_clv_12_months) as pLTV,
      avg(avg_session_value) as avg_avg_session_value,
      avg(profile_age_in_days) as avg_profile_age_in_days,
      avg(alive_probability) as avg_alive_probability,
      avg(days_since_last_active) as avg_days_since_last_active
    from
      attributed_clients
    group by
      country
    order by
      country
  ),

  alive_clients as (
    select country, count(*) as n
    from attributed_clients
    where user_status = 'Active'
    group by country
  ),

  e10s_clients as (
    select country, count(*) as n
    from attributed_clients
    where e10s_enabled = 'True'
    group by country
  ),

  fxa_clients as (
    select country, count(*) as n
    from attributed_clients
    where sync_configured = 'True'
    group by country
  ),

  default_browser_clients as (
    select country, count(*) as n
    from attributed_clients
    where is_default_browser = 'True'
    group by country
  )

select
  -- TODO: this is slightly off compared to the reference, how are clients being
  -- filtered?
  clients.*,
  alive_clients.n as num_alive_clients,
  alive_clients.n / clients.n as pct_alive,
  e10s_clients.n as num_e10s_enabled_clients,
  e10s_clients.n / clients.n as pct_e10s_enabled,
  fxa_clients.n as num_fxa_clients,
  fxa_clients.n / clients.n as pct_fxa,
  -- TODO: nocodes_pct
  default_browser_clients.n as num_default_browser_clients,
  default_browser_clients.n / clients.n as pct_default_browser
from
  clients
  join
    alive_clients
  on
    clients.country = alive_clients.country
  join
    e10s_clients
  on
    clients.country = e10s_clients.country
  join
    fxa_clients
  on
    clients.country = fxa_clients.country
  join
    default_browser_clients
  on
    clients.country = default_browser_clients.country
