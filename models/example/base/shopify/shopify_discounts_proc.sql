{% set stores = get_column_values(table=ref('stores_proc'), column='store_name', max_records=50, filter_column='platform', filter_value='Shopify') %}

{% if stores != [] %}

with orders as (

	{% for store in stores %}
		SELECT
		'{{store}}' store_name,
		created_at,
		doc_number as  order_number,
		null as  discount_code,
		null as discount_type,
		_fivetran_index as _sdc_sequence as _sdc_sequence,
		first_value(_fivetran_index) OVER (PARTITION BY doc_number ORDER BY _fivetran_index DESC) lv
		FROM `beaming-crowbar-330609.google_cloud_function_documents.transaction`  
		cross join unnest(discount_codes)
	
	{% if not loop.last %} UNION ALL {% endif %}
	{% endfor %}

)

SELECT
store_name,
order_number,
discount_code,
discount_type
FROM orders
where lv = _sdc_sequence

{% endif %}	
