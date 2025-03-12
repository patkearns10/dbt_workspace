with

sample_data as (
    select
        1 as unique_id,
        'good' as status,
        date('2024-01-01') as start_date,
        date('2024-01-02') as end_date,


    union all

    select
        2 as unique_id,
        'bad' as status,
        date('2024-01-02') as start_date,
        date('2024-01-03') as end_date,

    union all

    select
        3 as unique_id,
        'okay' as status,
        date('2024-01-01') as start_date,
        date('2024-01-04') as end_date,
)

select * from sample_data