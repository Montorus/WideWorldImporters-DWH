with source as (
    select * from {{ source('raw', 'raw_customers') }}
),

renamed as (
    select
        "CustomerID"                    as customer_id,
        "CustomerName"                  as customer_name,
        "CustomerCategoryID"            as customer_category_id,
        "CreditLimit"                   as credit_limit,
        "AccountOpenedDate"             as account_opened_date,
        "StandardDiscountPercentage"    as standard_discount_percentage,
        "IsStatementSent"               as is_statement_sent,
        "IsOnCreditHold"                as is_on_credit_hold,
        "PaymentDays"                   as payment_days,
        "PhoneNumber"                   as phone_number,
        "WebsiteURL"                    as website_url,
        "DeliveryAddressLine1"          as delivery_address,
        "DeliveryPostalCode"            as delivery_postal_code
    from source
    where "CustomerID" is not null
)

select * from renamed