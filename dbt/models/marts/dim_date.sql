with date_spine as (
    select generate_series(
        '2010-01-01'::date,
        '2030-12-31'::date,
        '1 day'::interval
    )::date as date_day
),

final as (
    select
        date_day                                        as date_id,
        date_day                                        as full_date,
        extract(year from date_day)::int                as year,
        extract(quarter from date_day)::int             as quarter,
        extract(month from date_day)::int               as month,
        to_char(date_day, 'Month')                      as month_name,
        extract(week from date_day)::int                as week_of_year,
        extract(day from date_day)::int                 as day_of_month,
        extract(dow from date_day)::int                 as day_of_week,
        to_char(date_day, 'Day')                        as day_name,
        case when extract(dow from date_day) in (0, 6)
            then true else false end                    as is_weekend,
        extract(year from date_day)::text || '-Q' ||
            extract(quarter from date_day)::text        as year_quarter,
        to_char(date_day, 'YYYY-MM')                    as year_month
    from date_spine
)

select * from final