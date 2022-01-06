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
        FROM {{ref('stripe')}}
        where fee>0 