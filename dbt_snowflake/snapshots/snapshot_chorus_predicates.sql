{% snapshot snapshot_chorus_predicates %}

{% set some_target_relation = api.Relation.create(database=database, schema=schema, identifier='upstream_example') -%}

    {{
        config(
            unique_key="unique_id",
            strategy='check',
            check_cols=['COLOR','_DATE'],
            custom_create_record_on_delete=true,
            custom_etl_at_ts = get_etl_at_timestamp(),
            target_predicates= ["exists (select 1 from " ~ some_target_relation ~ " s where DBT_INTERNAL_DEST.unique_id = s.unique_id)", "select 1 as col"],
        )
    }}

with
    source_example as (
        select * from {{ ref("upstream_example") }}
    ), 
    final as (

        select
            *,
            hash('{{ invocation_id }}')::number(38, 0) etl_proc_wid,
            current_timestamp(0)::timestamp_ntz w_insert_dt,
            current_timestamp(0)::timestamp_ntz w_update_dt
        from source_example
        where (1 = 1)
    )
select *
from final

{% endsnapshot %}