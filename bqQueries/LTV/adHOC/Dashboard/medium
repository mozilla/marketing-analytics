queries for segments, default browser, FxA, Multi-Desktop Sync, Mobile-Sync
--US AVG
--Select 

--From
--(
SELECT
medium, avg(total_clv) avg_tLTV, sum(total_clv)*100 sum_tLVT, avg(predicted_clv_12_months) avg_pLTV, sum(predicted_clv_12_months) sum_pLTV, count(distinct(client_id)) n, count(distinct(client_id))*100 population

FROM 
`ltv.ltv_v1` 

Group by 
medium

Order by
n desc
