select 
store,
account,
store_name,
source,
medium,
max(platform_n) platform,
max(channel_n) channel,
time_of_entry
from  ( 

SELECT  
'woocommerce' as store,
1 as account,
'qeresformacion' store_name,
dt.source,
medium,
'platform' as platform_n,
'channel' as channel_n,
_fivetran_synced as time_of_entry,
first_value(_fivetran_synced) OVER (PARTITION BY 'woocommerce' ORDER BY _fivetran_synced DESC) lv
FROM `beaming-crowbar-330609.google_analytics.google_analytics_custom_report` dt 

) 

WHERE lv = time_of_entry
group by store, account, store_name, source, medium, platform_n, channel_n, time_of_entry