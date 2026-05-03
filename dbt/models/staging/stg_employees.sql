with source as (
    select * from {{ source('raw', 'raw_employees') }}
),

renamed as (
    select
        businessentityid    as employee_id,
        nationalidnumber    as national_id,
        loginid             as login_id,
        jobtitle            as job_title,
        birthdate           as birth_date,
        maritalstatus       as marital_status,
        gender              as gender,
        hiredate            as hire_date,
        salariedflag        as is_salaried,
        vacationhours       as vacation_hours,
        sickleavehours      as sick_leave_hours,
        currentflag         as is_current,
        modifieddate        as modified_date
    from source
    where businessentityid is not null
)

select * from renamed