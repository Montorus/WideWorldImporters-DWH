with stg_customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        customer_id,
        customer_name,
        person_id,
        store_id,
        territory_id,
        account_number,
        first_name,
        middle_name,
        last_name,
        store_name,
        modified_date
    from stg_customers
)

select * from final
