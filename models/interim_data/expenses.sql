SELECT
    date as document_date,
    null as document_number,
    'facebook_marketing' as document_type,
    ROUND(sum(spend),2) as amount ,
    campaign_name as document_description,
    campaign_id as transactionid          
FROM {{ref('facebook_ads')}}
    WHERE spend>0
    GROUP BY 
    date,
    campaign_id,
    campaign_name

UNION ALL 

SELECT
    date as document_date,
    null as document_number,
    'adwords_marketing' as document_type,
    ROUND(sum(ad_cost),2) as amount ,
    null as document_description,
    cast (adwords_campaign_id AS FLOAT64) as transactionid  
FROM {{ref('ga_adwords_campaigns')}}
    WHERE ad_cost>0
    GROUP BY 
    date,
    adwords_campaign_id

UNION ALL 

SELECT 
    DATE(TIMESTAMP_SECONDS(dt.date)) as document_date,
    NULL as document_number,
    document_type,
    ROUND(amount,2)*-1 as amount ,
    dt.desc AS document_description,
    null as transactionid
FROM {{ref('holded_payroll')}} dt
    WHERE document_type='payroll'

UNION ALL 
              
SELECT 
    DATE(TIMESTAMP_SECONDS(dt.date)) as document_date,
    NULL as document_number,
    document_type,
    ROUND(total,2) as amount,
    dt.desc as document_description,
    null as transactionid
FROM {{ref('holded_documents')}} dt
    WHERE document_type='purchase'

UNION ALL 
              
   SELECT
        date(DATETIME(created, "Europe/Madrid")) as document_date,
        null as document_number,
        'stripe' as document_type,
        ROUND(((fee/100)),2) as amount,
        description as  document_description,
        null as transactionid
        FROM {{ref('stripe')}}
        where fee>0 