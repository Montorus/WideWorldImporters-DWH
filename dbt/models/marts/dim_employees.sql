with stg_employees as (
    select * from {{ ref('stg_employees') }}
),

final as (
    select
        employee_id,
        national_id,
        login_id,
        job_title,
        birth_date,
        marital_status,
        gender,
        hire_date,
        is_salaried,
        vacation_hours,
        sick_leave_hours,
        is_current,
        modified_date,
        -- SCD Type 2 fields
        current_timestamp   as valid_from,
        '9999-12-31'::date  as valid_to,
        true                as is_active
    from stg_employees
)

select * from final