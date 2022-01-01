with holded_orders as (
SELECT
  DATE(TIMESTAMP_SECONDS(dt.date)) AS document_date,
  TRIM(CAST(RIGHT(dt.DESC,6) AS STRING)) AS document_number,
  dt.document_type AS document_type,
  contact AS customer_id,
  dt.contact_name AS customer_name,
  currency,
  currency_change,
  JSON_VALUE(products,
    '$[0].name') AS product_name,
  total AS amount,
  SUBSTR(notes, 9, 10) AS payment_method
FROM
  `beaming-crowbar-330609`.`dbt_analiticaqeresformacion`.`holded_documents` dt
WHERE
  document_type='salesorder'
  AND DATE(TIMESTAMP_SECONDS(dt.date))<'2021-10-13'

UNION ALL

SELECT
  DATE(TIMESTAMP_SECONDS(dt.date)) AS document_date,
  TRIM(CAST(RIGHT(dt.DESC,6) AS STRING)) AS document_number,
  dt.document_type AS document_type,
  contact AS customer_id,
  dt.contact_name AS customer_name,
  currency,
  currency_change,
  JSON_VALUE(products,
    '$[0].name') AS product_name,
  total AS amount,
  SUBSTR(notes, 9, 10) AS payment_method
FROM
  `beaming-crowbar-330609`.`dbt_analiticaqeresformacion`.`holded_documents` dt
WHERE
  document_type='invoice'
  AND DATE(TIMESTAMP_SECONDS(dt.date))>='2021-10-13'
)

,landing_webinar_orders as (
SELECT
  DATE(Fecha_del_pedido) as order_date,
  CAST (N__mero_de_pedido AS STRING) as order_number,
  'landing_webinar' as order_source,
  null as order_client_id,
  CONCAT(Nombre__facturaci__n_,Apellidos__facturaci__n_) as order_client_name,
  null as order_currency,
  null as order_currency_rate, 
  Nombre_del_art__culo as order_product_name,
  Importe_total_del_pedido as order_amount,
  T__tulo_del_m__todo_de_pago as order_payment_method
  FROM   `beaming-crowbar-330609.google_cloud_function_documents.landing_webinar`

)

,landing_seminarios_orders as (
SELECT
  DATE(Fecha_del_pedido) as order_date,
  CAST (N__mero_de_pedido AS STRING) as order_number,
  'landing_seminario' as order_source,
  null as order_client_id,
  CONCAT(Nombre__facturaci__n_,Apellidos__facturaci__n_) as order_client_name,
  null as order_currency,
  null as order_currency_rate, 
  Nombre_del_art__culo as order_product_name,
  Importe_total_del_pedido as order_amount,
  case when T__tulo_del_m__todo_de_pago='Tarjeta de crédito/débito' then 'Redsys' else T__tulo_del_m__todo_de_pago end  as order_payment_method
  FROM `beaming-crowbar-330609.google_cloud_function_documents.landing_seminarios`

)

,landing_curso_orders as (
SELECT
  DATE(Fecha_del_pedido) as order_date,
  CAST (N__mero_de_pedido AS STRING) as order_number,
  'landing_curso' as order_source,
  null as order_client_id,
  CONCAT(Nombre__facturaci__n_,Apellidos__facturaci__n_) as order_client_name,
  null as order_currency,
  null as order_currency_rate, 
  Nombre_del_art__culo as order_product_name,
  Importe_total_del_pedido as order_amount,
  T__tulo_del_m__todo_de_pago as order_payment_method
  FROM `beaming-crowbar-330609.google_cloud_function_documents.landing_curso`
)
,web_antigua_orders as (
SELECT
  DATE(Fecha_del_pedido) as order_date,
  CAST (N__mero_de_pedido AS STRING) as order_number,
  'web_antigua' as order_source,
  null as order_client_id,
  CONCAT(Nombre__facturaci__n_,Apellidos__facturaci__n_) as order_client_name,
  null as order_currency,
  null as order_currency_rate, 
  Item_Name as order_product_name,
  Importe_total_del_pedido as order_amount,
  T__tulo_del_m__todo_de_pago as order_payment_method
  FROM `beaming-crowbar-330609.google_cloud_function_documents.web_antigua` 

)

SELECT 
*
FROM(
    SELECT
        document_date as order_date,
        document_number as order_number,
        'Holded' as order_source,
        --customer_id as order_client_id,
        customer_name as order_client_name,
        --currency as order_currency,
        --currency_change as order_currency_rate,
        product_name as order_product_name,
        amount as order_amount,
        payment_method as order_payment_method
    FROM holded_orders

    UNION ALL 

    SELECT
        order_date,
        order_number,
        order_source,
        --order_client_id,
        order_client_name,
        --order_currency,
        --order_currency_rate,
        order_product_name,
        order_amount,
        order_payment_method
    FROM landing_webinar_orders

    UNION ALL 

    SELECT
        order_date,
        order_number,
        order_source,
        --order_client_id,
        order_client_name,
        --order_currency,
        --order_currency_rate,
        order_product_name,
        order_amount,
        order_payment_method
    FROM landing_seminarios_orders

    UNION ALL 

    SELECT
        order_date,
        order_number,
        order_source,
        --order_client_id,
        order_client_name,
        --order_currency,
        --order_currency_rate,
        order_product_name,
        order_amount,
        order_payment_method
    FROM landing_curso_orders
    where order_number is not null

    UNION ALL 

    SELECT
        order_date,
        order_number,
        order_source,
        --order_client_id,
        order_client_name,
        --order_currency,
        --order_currency_rate,
        order_product_name,
        order_amount,
        order_payment_method
    FROM web_antigua_orders)
Order by order_date desc,order_number desc