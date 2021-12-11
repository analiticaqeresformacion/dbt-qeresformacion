select 
store,
store_name,
account,
platform,
max(time_of_entry) time_of_entry

from  ( 

SELECT  
'woocommerce' as store,
'bigquery_name' as store_name,
1 as account,
'platform' as platform,
_fivetran_synced as time_of_entry,
first_value(_fivetran_synced) OVER (PARTITION BY 'woocommerce' ORDER BY _fivetran_synced DESC) lv
FROM `beaming-crowbar-330609.google_analytics.google_analytics_custom_report`

) 

WHERE lv = time_of_entry
group by store, store_name, account, platform