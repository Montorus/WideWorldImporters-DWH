with stg_customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        customer_id,
        customer_name,
        customer_category_id,
        credit_limit,
        account_opened_date,
        standard_discount_percentage,
        is_statement_sent,
        is_on_credit_hold,
        payment_days,
        phone_number,
        website_url,
        delivery_address,
        delivery_postal_code,
        -- SCD Type 2 fields
        current_timestamp   as valid_from,
        '9999-12-31'::date  as valid_to,
        true                as is_current
    from stg_customers
)

select * from final