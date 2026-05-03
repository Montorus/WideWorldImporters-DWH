with stg_sales_orders as (
    select * from {{ ref('stg_sales_orders') }}
),

stg_sales_details as (
    select * from {{ ref('stg_sales_details') }}
),

final as (
    select
        -- Keys
        sd.sales_order_id,
        sd.sales_order_detail_id,
        so.customer_id,
        sd.product_id,
        so.salesperson_id           as employee_id,
        so.order_date::date         as date_id,

        -- Measures
        sd.order_quantity,
        sd.unit_price,
        sd.unit_price_discount,
        sd.line_total,
        so.subtotal,
        so.tax_amount,
        so.freight,
        so.total_due,

        -- Additional info
        so.order_status,
        so.is_online_order,
        so.ship_date,
        so.due_date

    from stg_sales_details sd
    left join stg_sales_orders so
        on sd.sales_order_id = so.sales_order_id
)

select * from final