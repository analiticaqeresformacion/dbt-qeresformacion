Select 
document_date,
document_number,
document_type,
customer_id,
customer_name,
currency,
currency_change,
product_name,
amount ,
transaction_revenue,
quantity,
hostname,
campaign,
transactionid,
url,
medium,
source ,
payment_method,

 from (  
        
        SELECT DATE(TIMESTAMP_SECONDS(dt.date)) as document_date,
                        dt.desc as document_number,
                        document_type,
                        contact_id as customer_id,
                        contact_name as customer_name,
                        null as currency,
                        null as currency_change,
                        null as product_name,
                        amount ,
                        null as transaction_revenue,
                        null as quantity,
                        null as hostname,
                        null as campaign,
                        null as transactionid,
                        null as url,
                        null as medium,
                        null as source ,
                        null as payment_method
                        FROM {{ref('holded_documents')}} dt
                        where document_type='payroll'

              UNION ALL 
              
               SELECT DATE(TIMESTAMP_SECONDS(dt.date)) as document_date,
                        dt.desc as document_number,
                        document_type,
                        contact_id as customer_id,
                        contact_name as customer_name,
                        null as currency,
                        null as currency_change,
                        null as product_name,
                        amount ,
                        null as transaction_revenue,
                        null as quantity,
                        null as hostname,
                        null as campaign,
                        null as transactionid,
                        null as url,
                        null as medium,
                        null as source ,
                        null as payment_method
                        FROM {{ref('holded_documents')}} dt
                        where document_type='purchase'
      

       UNION ALL 

       SELECT
       date as document_date,
       'campaign_id' as document_number,
       'facebook_marketing' as document_type,
       null as customer_id,
       null as customer_name,
       null as currency,
       null as currency_change,
       null as product_name,
       sum(spend) as amount ,
       sum(spend) as transaction_revenue,
       0 as quantity,
       null as hostname,
       campaign_name as campaign,
       campaign_id as transactionid,
       null as url,
       null as medium,
       null as source ,
       null as payment_method                   
       FROM
       {{ref('facebook_ads')}}
       where spend>0
       GROUP BY 
       date,
       campaign_id,
       campaign_name

       UNION ALL 
       SELECT
       date as document_date,
       adwords_campaign_id as document_number,
       'adwords_marketing' as document_type,
       null as customer_id,
       null as customer_name,
       null as currency,
       null as currency_change,
       null as product_name,
       sum(ad_cost) as amount ,
       sum(ad_cost) as transaction_revenue,
       0 as quantity,
       null as hostname,
       adwords_campaign_id as campaign,
       null as transactionid,
       null as url,
       null as medium,
       null as source ,
       null as payment_method                   
       FROM
       {{ref('ga_adwords_campaigns')}}
       WHERE ad_cost>0
       GROUP BY 
       date,
       adwords_campaign_id

        UNION ALL

        SELECT 
        date as document_date,
       document_number as document_number,
       'stripe' as document_type,
       null as customer_id,
       null as customer_name,
       null as currency,
       null as currency_change,
       null as product_name,
       fee as amount ,
       fee as transaction_revenue,
       0 as quantity,
       null as hostname,
       NULL as campaign,
       null as transactionid,
       null as url,
       null as medium,
       null as source ,
       null as payment_method 

        from (
        SELECT
        date(DATETIME(created, "Europe/Madrid")) as date,
        description,
        status,
        type,
        case when type='charge' then right(description,5) else left(right(description,6),5) end as document_number,
        amount/100 as amount,
        (fee/100)*-1 as fee,
        net/100 as net,
        currency,
        exchange_rate
        FROM `beaming-crowbar-330609.stripe.balance_transaction`
        where fee>0 ) stripe


        UNION ALL

        SELECT document_date ,
        document_number,
        document_type,
        customer_id,
        customer_name,
        currency,
        currency_change,
        product_name,
        amount,
        transaction_revenue,
        transactions as quantity,
        hostname,
        lower(replace(replace(replace(campaign,' ', ''),'-',''),'_','')) campaign,
        cast(regexp_replace(r.transaction_id, r'#|B', '') as int64) transactionid,
        lower(trim(regexp_replace(replace(replace(replace(replace(CONCAT(hostname,landing_page_path),'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),'/')) as url,
        medium,
        source,
        payment_method
        FROM (
                    SELECT
                    DATE(TIMESTAMP_SECONDS(dt.date)) as document_date,
                    RIGHT(dt.desc,6) as document_number,
                    dt.document_type  as document_type,
                    contact as customer_id,
                    dt.contact_name as customer_name,
                    currency,
                    currency_change,
                    JSON_VALUE(products,'$[0].name') as product_name,
                    total as amount ,
                    SUBSTR(notes, 9, 10) AS payment_method
                    FROM {{ref('holded_documents')}} dt 
                    where document_type='salesorder' and DATE(TIMESTAMP_SECONDS(dt.date))<'2021-10-13'

                    UNION ALL 

                    SELECT
                    DATE(TIMESTAMP_SECONDS(dt.date)) as document_date,
                    RIGHT(dt.desc,6) as document_number,
                    dt.document_type  as document_type,
                    contact as customer_id,
                    dt.contact_name as customer_name,
                    currency,
                    currency_change,
                    JSON_VALUE(products,'$[0].name') as product_name,
                    total as amount ,
                    SUBSTR(notes, 9, 10) AS payment_method
                    FROM {{ref('holded_documents')}} dt 
                    where document_type='invoice' and DATE(TIMESTAMP_SECONDS(dt.date))>='2021-10-13'


        
        
        ) dt
        left join {{ref('ga_main')}} r on r.transaction_id=dt.document_number 
        
        ) financial_Data


        order by document_date desc, document_number desc