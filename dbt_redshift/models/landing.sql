with sample_data as (

    select
    1 as profile_id,
    null as external_id,
    'sample@gmail.com' as email,
    '2021-01-04T00:07:43.805800+00:00' as signup,
    '2023-10-01T00:07:43.805800+00:00' as api_loaded_at

    union all
    
    select
    1 as profile_id,
    null as external_id,
    null as email,
    '2021-01-04T00:07:43.805800+00:00' as signup,
    '2023-10-02T00:07:43.805800+00:00' as api_loaded_at

    union all
    
    select
    1 as profile_id,
    2 as external_id,
    null as email,
    '2021-01-04T00:07:43.805800+00:00' as signup,
    '2023-10-03T00:07:43.805800+00:00' as api_loaded_at

)

select * from sample_data