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
        modified_date
    from stg_employees
)

select * from final
