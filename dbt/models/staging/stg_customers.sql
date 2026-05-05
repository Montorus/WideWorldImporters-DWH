with source as (
    select * from {{ source('raw', 'raw_customers') }}
),

renamed as (
    select
        customerid                  as customer_id,
        customer_name               as customer_name,
        personid                    as person_id,
        storeid                     as store_id,
        territoryid                 as territory_id,
        accountnumber               as account_number,
        first_name                  as first_name,
        middle_name                 as middle_name,
        last_name                   as last_name,
        store_name                  as store_name,
        modifieddate                as modified_date
    from source
    where customerid is not null
)

select * from renamed
