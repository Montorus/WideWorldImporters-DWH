with source as (
    select * from {{ source('raw', 'raw_products') }}
),

renamed as (
    select
        productid               as product_id,
        name                    as product_name,
        productnumber           as product_number,
        color                   as color,
        safetystocklevel        as safety_stock_level,
        reorderpoint            as reorder_point,
        standardcost            as standard_cost,
        listprice               as list_price,
        size                    as size,
        weight                  as weight,
        daystomanufacture       as days_to_manufacture,
        productline             as product_line,
        class                   as product_class,
        style                   as style,
        makeflag                as is_make_item,
        finishedgoodsflag       as is_finished_good
    from source
    where productid is not null
)

select * from renamed