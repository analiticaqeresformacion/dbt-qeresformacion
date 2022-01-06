select * from (

SELECT
fb.date as campaign_date,
'facebook' as campaign_type,
campaign_id,
campaign_name,
round(sum(spend),2) AS campaign_spend
FROM   {{ref('facebook_ads')}} fb
GROUP BY  
fb.date,
campaign_id,
campaign_name

UNION ALL

SELECT 
ga.date as campaign_date,
'google_ads' as campaign_type,
campaign_id,
campaign_name,
round(sum(cost),2) AS campaign_spend
 FROM {{ref('google_ads')}}  ga
 WHERE cost>0
 GROUP BY 
 ga.date,
campaign_id,
campaign_name )

order by campaign_date desc