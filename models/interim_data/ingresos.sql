SELECT  
o.date_created as order_date,
o.order_id as order_number,
'web_woocommerce' as order_source,
o.customer_id,
CONCAT(first_name,' ',last_name) as order_client_name,
oi.order_item_name as order_product_name,
total_sales,
coupon_amount,
tax_amount,
net_total,
status
FROM {{ref('wp_wc_order_stats')}} o
LEFT JOIN {{ref('wp_wc_order_product_lookup')}}  p on o.order_id = p.order_id
LEFT JOIN {{ref('wp_wc_order_items')}} oi on oi.order_id=o.order_id and order_item_type='line_item'
LEFT JOIN {{ref('wp_wc_customer_lookup')}} c on c.customer_id = o.customer_id
Where o.order_id is not null
and (status = 'wc-completed')
order by o.order_id