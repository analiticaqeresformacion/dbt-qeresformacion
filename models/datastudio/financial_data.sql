SELECT 
document_date,
document_number,
document_type,
document_financial_type,
document_customer,
document_description,
 document_amount ,
document_payment_method

FROM (


SELECT
       order_date as document_date,
       safe_cast(order_number AS FLOAT64) as document_number,
       'Ingresos' as document_financial_type,
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
       'Gastos' as document_financial_type,
       document_type,
       null as document_customer,
       document_description,
       amount as document_amount ,
       null as document_payment_method
FROM {{ref('expenses')}}
)