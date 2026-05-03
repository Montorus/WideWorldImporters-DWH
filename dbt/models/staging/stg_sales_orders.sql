with source as (
    select * from {{ source('raw', 'raw_sales_orders') }}
),

renamed as (
    select
        salesorderid            as sales_order_id,
        revisionnumber          as revision_number,
        orderdate               as order_date,
        duedate                 as due_date,
        shipdate                as ship_date,
        status                  as order_status,
        onlineorderflag         as is_online_order,
        purchaseordernumber     as purchase_order_number,
        accountnumber           as account_number,
        customerid              as customer_id,
        salespersonid           as salesperson_id,
        territoryid             as territory_id,
        shipmethodid            as ship_method_id,
        creditcardid            as credit_card_id,
        currencyrateid          as currency_rate_id,
        subtotal                as subtotal,
        taxamt                  as tax_amount,
        freight                 as freight,
        totaldue                as total_due,
        orderdate::date         as order_date_key
    from source
    where salesorderid is not null
)

select * from renamed