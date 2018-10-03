select count(client_id) ltv_clients, sum(if(p_client_id is null, 1,0)) missing_from_palomar, sum(if(p_client_id is null, 1,0))/count(client_id) pct_missing
From ltv.ltv_palomar