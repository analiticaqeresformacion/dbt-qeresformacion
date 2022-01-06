SELECT 
date as order_date,
transaction_id as order_number,
transaction_revenue,
campaign,
hostname,
landing_page_path,
medium,
source
FROM {{ref('ga_main')}}