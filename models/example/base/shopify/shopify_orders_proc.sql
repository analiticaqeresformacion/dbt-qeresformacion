 -- depends_on: {{ ref('stores_proc') }}, {{ ref('shopify_refunds_proc')}}, {{ ref('shopify_discounts_proc')}}

{% set stores = get_column_values(table=ref('stores_proc'), column='store_name', max_records=50, filter_column='platform', filter_value='Shopify') %}

{% if stores != [] %}

with orders as (

	{% for store in stores %}
	SELECT 
	store_name,
	lookup_platform,
	created_at,
	order_number,
	total_order_price_undiscounted,
	total_discounts,
	discount_pct * price * quantity as discount,
	total_order_shipping_price,
	total_order_price_incl_shipping,
	checkout_id,
	landing_site,
	id line_item_id,    
	customer_id,    
	quantity,
	case when discount_pct > 0 then (price * quantity) * (1 - discount_pct) 
	  else price * quantity end as price,
	product_id, 
	sku, 
	variant_title, 
	variant_id,
	_sdc_sequence,
	lv
	FROM (

		SELECT
		'{{store}}' store_name,
		'woocommerce' as lookup_platform,
    	DATE(TIMESTAMP_SECONDS(date)) as  created_at,
		document_id as  order_number,
    	0 as  total_order_price_undiscounted,
    	0 as total_discounts,
	    null as  discount_pct,        	
		null as total_order_shipping_price,
		amount as total_order_price_incl_shipping,
    	null as checkout_id,
    	null as landing_site,    
		contact as customer_id,    
    	null as line_items,
    	document_type as financial_status,
		_fivetran_index as _sdc_sequence,
		first_value(_fivetran_index) OVER (PARTITION BY document_idORDER BY _fivetran_index DESC) lv
		FROM `beaming-crowbar-330609.google_cloud_function_documents.transaction` 
		cross join unnest(shipping_lines)
		where financial_status in ('salesorder')
	)
	cross join unnest(line_items)
	where lv = _sdc_sequence
	
	{% if not loop.last %} UNION ALL {% endif %}
	{% endfor %}

)

SELECT
b.account,
b.store,
b.platform,
created_at,
a.order_number,
a.quantity,
c.quantity refund_quantity,
case when c.quantity is not null then a.quantity - c.quantity else a.quantity end as final_quantity,
price, 
total_order_price_undiscounted,
total_discounts,
trim(lower(d.discount_code)) discount_code,
discount,
d.discount_type,
total_order_shipping_price,
total_order_price_incl_shipping,
refund_amount,
case when refund_amount is not null then price - refund_amount else price end as final_price,
a.checkout_id,
a.product_id, 
landing_site,
sku, 
variant_title, 
a.variant_id,
a.line_item_id,	
customer_id
FROM orders a
LEFT JOIN {{ref('stores_proc')}} b 
ON ( a.store_name = b.store_name
  AND a.lookup_platform = b.platform )
LEFT JOIN {{ref('shopify_refunds_proc')}} c
ON ( a.order_number = c.order_number
	AND a.line_item_id = c.line_item_id
	AND a.store_name = c.store_name )
LEFT JOIN {{ref('shopify_discounts_proc')}} d
ON ( a.order_number = d.order_number 
    AND a.store_name = d.store_name )  	

{% endif %}	
