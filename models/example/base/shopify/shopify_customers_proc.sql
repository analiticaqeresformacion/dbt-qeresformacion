 -- depends_on: {{ ref('stores_proc') }}

{% set stores = get_column_values(table=ref('stores_proc'), column='store_name', max_records=50, filter_column='platform', filter_value='Shopify') %}

{% if stores != [] %}

with customers as (

	{% for store in stores %}
	SELECT
	'{{store}}' store_name,
	'woocommerce' as lookup_platform,
	DATE(TIMESTAMP_SECONDS(date))  as created_at,
	contact as id,
	contact_name as first_name,
	null as last_name,
	null as email,
	_fivetran_index as _sdc_sequence,
	first_value(_fivetran_index) over (partition by contact order by _fivetran_index desc) lv
	FROM `beaming-crowbar-330609.google_cloud_function_documents.transaction` 
    where document_type='salesorder'
	{% if not loop.last %} UNION ALL {% endif %}
	{% endfor %}

)

SELECT
b.account,
b.store,
b.platform,
created_at,
id,
first_name,
last_name,
email
FROM customers a
LEFT JOIN {{ref('stores_proc')}} b 
ON ( a.store_name = b.store_name
  AND a.lookup_platform = b.platform )
where a.lv = a._sdc_sequence

{% endif %}	 
