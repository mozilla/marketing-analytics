select corr(total_clv,days_active) cc_tLTV_Days_active, corr(total_clv,days_dau) cc_tLTV_Days_dau
From ltv.ltv_palomar
Where p_client_id is not null
and country like 'US'
and historical_searches < (
    SELECT
      STDDEV(historical_searches)
    FROM
      `ltv.ltv_v1`) *2.5 + (
    SELECT
      AVG(historical_searches)
    FROM
      `ltv.ltv_v1`)
