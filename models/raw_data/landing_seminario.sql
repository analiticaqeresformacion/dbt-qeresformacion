SELECT
  DATE(Fecha_del_pedido) as order_date,
  CAST (N__mero_de_pedido AS STRING) as order_number,
  'landing_seminario' as order_source,
  null as order_client_id,
  CONCAT(Nombre__facturaci__n_,' ',Apellidos__facturaci__n_) as order_client_name,
  null as order_currency,
  null as order_currency_rate, 
  Nombre_del_art__culo as order_product_name,
  Importe_total_del_pedido as order_amount,
  case when T__tulo_del_m__todo_de_pago='Tarjeta de crédito/débito' then 'Redsys' else T__tulo_del_m__todo_de_pago end  as order_payment_method
  FROM `beaming-crowbar-330609.google_cloud_function_documents.raw_landing_seminario`