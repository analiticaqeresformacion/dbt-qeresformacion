SELECT
store,
client_name,
order_number,
order_date,
recent_order_date,
first_order_date,
case when first_order_number = order_number then 'New'
	when date_diff(order_date, recent_order_date, DAY) <= 365 then 'Repeat'
	when date_diff(order_date, recent_order_date, DAY) > 365 then 'Reactivated'
 else '' end as order_type,
quantity,
revenue,
1 as orders,
first_order_revenue,
lifetime_revenue
FROM

(

	SELECT
	'woocommerce' as store,
	customer_name as client_name,
	document_number as order_number,
	document_date as order_date,
	1 as quantity,
	amount as revenue,
	lag(DATE(document_date)) over (PARTITION BY customer_name ORDER BY document_date asc) recent_order_date,
	first_value(DATE(document_date)) over (PARTITION BY customer_name ORDER BY document_date asc) first_order_date,
	first_value(document_number) over (PARTITION BY customer_name ORDER BY document_date asc) first_order_number,
	first_value(amount) over (PARTITION BY customer_name ORDER BY document_date asc) first_order_revenue,
	sum(amount) over (PARTITION BY customer_name) lifetime_revenue
	FROM (select *   
                FROM (
                    SELECT
                    DATE(TIMESTAMP_SECONDS(dt.date)) as document_date,
                    TRIM(CAST(RIGHT(dt.desc,6) AS STRING)) as document_number,
                    dt.document_type  as document_type,
                    contact as customer_id,
                    dt.contact_name as customer_name,
                    currency,
                    currency_change,
                    JSON_VALUE(products,'$[0].name') as product_name,
                    total as amount ,
                    SUBSTR(notes, 9, 10) AS payment_method
                    FROM {{ref('holded_documents')}} dt 
                    where document_type='salesorder' and DATE(TIMESTAMP_SECONDS(dt.date))<'2021-10-13'

                    UNION ALL 

                    SELECT
                    DATE(TIMESTAMP_SECONDS(dt.date)) as document_date,
                    TRIM(CAST(RIGHT(dt.desc,6) AS STRING)) as document_number,
                    dt.document_type  as document_type,
                    contact as customer_id,
                    dt.contact_name as customer_name,
                    currency,
                    currency_change,
                    JSON_VALUE(products,'$[0].name') as product_name,
                    total as amount ,
                    SUBSTR(notes, 9, 10) AS payment_method
                    FROM {{ref('holded_documents')}} dt 
                    where document_type='invoice' and DATE(TIMESTAMP_SECONDS(dt.date))>='2021-10-13'
                        )
) )
order by client_name