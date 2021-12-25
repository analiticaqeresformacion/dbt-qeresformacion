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
	lag(DATE(TIMESTAMP_SECONDS(document_date))) over w1 recent_order_date,
	first_value(DATE(TIMESTAMP_SECONDS(document_date))) over w1 first_order_date,
	first_value(document_number) over w1 first_order_number,
	first_value(amount) over w1 first_order_revenue,
	sum(amount) over w2 lifetime_revenue
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
	WINDOW w1 as (PARTITION BY  contact_name ORDER BY DATE(TIMESTAMP_SECONDS(date)) asc),
	w2 as (PARTITION BY contact_name)
) )
order by client_name