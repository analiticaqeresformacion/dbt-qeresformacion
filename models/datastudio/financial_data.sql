SELECT 
document_date,
document_number,
document_type,
case when document_type IN ('web_antigua','landing_seminario','landing_webinar','Holded','landing_curso') then 'Ingresos'
     when document_type IN ('adwords_marketing','facebook_marketing') then 'Gastos Marketing'
     when document_type IN ('payroll')  then 'Gastos Sueldos' 
    else 'Otros Gastos'  
    end as document_financial_type,
document_customer,
document_description,
 document_amount ,
document_payment_method

FROM (


SELECT
       order_date as document_date,
       safe_cast(order_number AS FLOAT64) as document_number,
       order_source as document_type,
       order_client_name as document_customer,
       order_product_name as document_description,
       order_amount as document_amount ,
       order_payment_method as document_payment_method
FROM {{ref('pedidos')}}

UNION ALL 

SELECT
       document_date,
       safe_cast (document_number as FLOAT64) AS document_number ,
       document_type,
       null as document_customer,
       document_description,
       amount as document_amount ,
       null as document_payment_method
FROM {{ref('expenses')}}
)