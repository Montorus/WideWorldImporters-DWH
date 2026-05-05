with stg_products as (
    select * from {{ ref('stg_products') }}
),

final as (
    select
        product_id,
        product_name,
        product_number,
        color,
        safety_stock_level,
        reorder_point,
        standard_cost,
        list_price,
        size,
        weight,
        days_to_manufacture,
        product_line,
        product_class,
        style,
        is_make_item,
        is_finished_good
    from stg_products
)

select * from final
