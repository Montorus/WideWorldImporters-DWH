with source as (
    select * from {{ source('raw', 'raw_sales_details') }}
),

renamed as (
    select
        salesorderid            as sales_order_id,
        salesorderdetailid      as sales_order_detail_id,
        carriertrackingnumber   as carrier_tracking_number,
        orderqty                as order_quantity,
        productid               as product_id,
        specialofferid          as special_offer_id,
        unitprice               as unit_price,
        unitpricediscount       as unit_price_discount,
        -- Calculate line total
        orderqty * unitprice * (1 - unitpricediscount) as line_total,
        modifieddate            as modified_date
    from source
    where salesorderid is not null
    and salesorderdetailid is not null
)

select * from renamed