with

sample_data as (
    select
        1 as unique_id,
        'red' as color,
        date('2024-01-01') as _date

    union all

    select
        1 as unique_id,
        'orange' as color,
        date('2024-01-02') as _date

    union all

    select
        1 as unique_id,
        'yellow' as color,
        date('2024-01-03') as _date
    
    union all

    select
        2 as unique_id,
        'green' as color,
        date('2024-01-04') as _date
    
    union all

    select
        2 as unique_id,
        'blue' as color,
        date('2024-01-05') as _date
    
    union all

    select
        3 as unique_id,
        'indigo' as color,
        date('2024-01-06') as _date
    
    union all

    select
        2 as unique_id,
        'violet' as color,
        date('2024-01-07') as _date

    union all

    select
        3 as unique_id,
        'cyan' as color,
        date('2024-01-08') as _date
    
    union all

    select
        2 as unique_id,
        'white' as color,
        date('2024-01-09') as _date
    
    union all

    select
        4 as unique_id,
        'white' as color,
        date('2024-01-10') as _date

    union all

    select
        5 as unique_id,
        'white' as color,
        date('2024-01-11') as _date
)

select * from sample_data