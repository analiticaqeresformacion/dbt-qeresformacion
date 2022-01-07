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
 orders,
first_order_revenue,
round(lifetime_revenue,2) as lifetime_revenue
FROM (
	SELECT
        'woocommerce' as store,
        order_client_name as client_name,
        order_number,
        order_date,
        1 as quantity,
        order_amount as revenue,
        count(order_number) over (PARTITION BY order_client_name) orders,
        lag(DATE(order_date)) over (PARTITION BY order_client_name ORDER BY order_date asc) recent_order_date,
        first_value(DATE(order_date)) over (PARTITION BY order_client_name ORDER BY order_date asc) first_order_date,
        first_value(order_number) over (PARTITION BY order_client_name ORDER BY order_date asc) first_order_number,
        first_value(order_amount) over (PARTITION BY order_client_name ORDER BY order_date asc) first_order_revenue,
        sum(order_amount) over (PARTITION BY order_client_name) lifetime_revenue
	FROM {{ref('pedidos')}}
)
order by client_name