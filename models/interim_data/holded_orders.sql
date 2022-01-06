 SELECT 
 MAX(document_date) as document_date,
 document_number,
 MIN(document_type) as document_type,
 customer_id,
 customer_name,
 MIN(currency) AS currency,
 MIN(currency_change) as currency_change,
 product_name,
 max(amount) as amount,
 MAX(payment_method) AS payment_method
 FROM ( 
  
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
  FROM {{ref('holded_documents')}} dt
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
  FROM {{ref('holded_documents')}} dt
  WHERE
    document_type='invoice'
    AND DATE(TIMESTAMP_SECONDS(dt.date))>='2021-10-13' ) holded

GROUP BY
 document_number,
 customer_id,
 customer_name,
 product_name