SELECT
  DATE(fecha_pedido) AS order_date,
  numero_pedido as order_number,
  'web_woocommerce' as order_source,
   CONCAT(nombre_facturacion,' ',apellido_facturacion) as order_client_name,
  null as order_currency,
  null as order_currency_rate, 
  nombre_articulo as order_product_name,
  importe_total_del_pedido as order_amount,
  case when titulo_del_metodo_de_pago='Pago con tarjeta' then 'redsis' else titulo_del_metodo_de_pago end  as order_payment_method
FROM
  `beaming-crowbar-330609.google_cloud_function_documents.raw_web_woocommerce`