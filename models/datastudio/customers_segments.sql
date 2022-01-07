SELECT 
client_name,
max(orders) total_compras,
min(first_order_date) as first_order,
max(order_date) as last_order,
min(first_order_revenue) as first_order_revenue,
max(lifetime_revenue) as lifetime_revenue,
case when max(lifetime_revenue)>3000 then '+3000' 
 when max(lifetime_revenue)<3000 and max(lifetime_revenue)>1000 then '+1000'
    when max(lifetime_revenue)<1000 and max(lifetime_revenue)>500 then '+500'
    else '-500'
    end as segment_revenue,
case when sum(orders)=1 then '1'
when sum(orders)=2 then '2'
else '3+'
 end as segment_frecuency
FROM {{ref('customers_transactions')}} t
group by client_name