 SELECT
 order_id,
  parent_id,
  date_created,
  date_created_gmt,
  num_items_sold,
  total_sales,
  tax_total,
  shipping_total,
  net_total,
  status,
  customer_id
FROM `beaming-crowbar-330609.google_cloud_function_documents.raw_wp_wc_order_stats` 