with calc_employees as (
    select 
    -- date_part(year, birth_date),
    date_part_year(current_date) - date_part_year(birth_date) as age,
    date_part_year(current_date) - date_part_year(hire_date) as length_of_service,
    first_name || ' ' || last_name as name, *
    from {{source('sources','employees')}}
)

select * from calc_employees