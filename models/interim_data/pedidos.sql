with ga_main_agg as (
    SELECT 
        transaction_id,
        r.hostname,
        lower(replace(replace(replace(r.campaign,' ', ''),'-',''),'_','')) as campaign,
        lower(trim(regexp_replace(replace(replace(replace(replace(CONCAT(r.hostname,r.landing_page_path),'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),'/')) as url,
        r.medium,
        r.source
    FROM {{ref('ga_main')}} r
    GROUP BY 
    transaction_id,
    hostname,
    campaign,
    url,
    medium,
    source)

    SELECT 
        pedidos.*,
        hostname,
        campaign,
        url,
        medium,
        source
    FROM(

        SELECT
            document_date as order_date,
            document_number as order_number,
            'Holded' as order_source,
            --customer_id as order_client_id,
            customer_name as order_client_name,
            --currency as order_currency,
            --currency_change as order_currency_rate,
            product_name as order_product_name,
            amount as order_amount,
            payment_method as order_payment_method
        FROM {{ref('holded_orders')}} 
            WHERE document_date>='2021-12-27'

    UNION ALL

        SELECT
            order_date,
            cast(order_number as string) as order_number,
            order_source,
            --order_client_id,
            order_client_name,
            --order_currency,
            --order_currency_rate,
            MAX(order_product_name) aS order_product_name,
            order_amount,
            order_payment_method
        FROM {{ref('web_woocommerce')}}
        where order_product_name is not null
        GROUP BY 
            order_date,
            order_number,
            order_source,
            order_client_name,
            order_amount,
            order_payment_method

        UNION ALL 

        SELECT
            order_date,
            order_number,
            order_source,
            --order_client_id,
            order_client_name,
            --order_currency,
            --order_currency_rate,
            MAX(order_product_name),
            order_amount,
            order_payment_method
        FROM {{ref('landing_webinar')}} 
        GROUP BY 
         order_date,
            order_number,
            order_source,
            order_client_name,
            order_amount,
            order_payment_method

        UNION ALL 

        SELECT
            order_date,
            order_number,
            order_source,
            --order_client_id,
            order_client_name,
            --order_currency,
            --order_currency_rate,
            MAX(order_product_name),
            order_amount,
            order_payment_method
        FROM {{ref('landing_seminario')}} 
        GROUP BY 
            order_date,
            order_number,
            order_source,
            order_client_name,
            order_amount,
            order_payment_method

        UNION ALL 

        SELECT
            order_date,
            order_number,
            order_source,
            --order_client_id,
            order_client_name,
            --order_currency,
            --order_currency_rate,
            MAX(order_product_name),
            order_amount,
            order_payment_method
        FROM {{ref('landing_curso')}}
        where order_number is not null
        GROUP BY 
            order_date,
            order_number,
            order_source,
            order_client_name,
            order_amount,
            order_payment_method

        UNION ALL 

        SELECT
            order_date,
            order_number,
            order_source,
            --order_client_id,
            order_client_name,
            --order_currency,
            --order_currency_rate,
            MAX(order_product_name),
            order_amount,
            order_payment_method
        FROM {{ref('web_antigua')}}
        where order_product_name is not null
        GROUP BY 
            order_date,
            order_number,
            order_source,
            order_client_name,
            order_amount,
            order_payment_method


    ) pedidos

LEFT JOIN ga_main_agg ga on ga.transaction_id=pedidos.order_number 