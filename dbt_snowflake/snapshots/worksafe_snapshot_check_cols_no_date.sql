{% snapshot worksafe_snapshot_check_cols_no_date %}
    {{
    config(
      target_database='development',
      target_schema='dbt_pkearns',
      unique_key='sk',
      strategy='check',
      check_cols=['full_hash']
    )
}}

with source as (
    select
        '2025-03-01' as omd_insert_datetime,
        'aaa' as sk,
        'code' as business_key,
        'U' as cdc_operation,
        'bbb' as business_column,
        'xxx' as columns_not_modeled,
        concat(sk,'|', business_key,'|', cdc_operation,'|', business_column,'|', columns_not_modeled) as full_hash

    union all 
    select
        '2025-03-02' as omd_insert_datetime,
        'aaa' as sk,
        'code' as business_key,
        'D' as cdc_operation,
        'bbb1' as business_column,
        'xxx1' as columns_not_modeled,
        concat(sk,'|', business_key,'|', cdc_operation,'|', business_column,'|', columns_not_modeled) as full_hash

    union all 
    select
        '2025-03-03' as omd_insert_datetime,
        'aaa' as sk,
        'code' as business_key,
        'I' as cdc_operation,
        'bbb' as business_column,
        'xxx' as columns_not_modeled,
        concat(sk,'|', business_key,'|', cdc_operation,'|', business_column,'|', columns_not_modeled) as full_hash

    union all 
    select
        '2025-03-04' as omd_insert_datetime,
        'aaa' as sk,
        'code' as business_key,
        'U' as cdc_operation,
        'bbb2' as business_column,
        'xxx3' as columns_not_modeled,
        concat(sk,'|', business_key,'|', cdc_operation,'|', business_column,'|', columns_not_modeled) as full_hash

    union all 
    select
        '2025-03-04' as omd_insert_datetime,
        'aaa' as sk,
        'code' as business_key,
        'U' as cdc_operation,
        'bbb2' as business_column,
        'xxx3' as columns_not_modeled,
        concat(sk,'|', business_key,'|', cdc_operation,'|', business_column,'|', columns_not_modeled) as full_hash

    union all
    select
        '2025-03-05' as omd_insert_datetime,
        'aaa' as sk,
        'code' as business_key,
        'U' as cdc_operation,
        'bbb2' as business_column,
        'xxx3' as columns_not_modeled,
        concat(sk,'|', business_key,'|', cdc_operation,'|', business_column,'|', columns_not_modeled) as full_hash
)

select * from source

{% endsnapshot %}