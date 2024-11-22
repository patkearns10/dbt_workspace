{% snapshot dbt_artifacts_tests %}
    {{
        config(
            target_database='development',
            target_schema='dbt_pkearns_snapshots',
            unique_key='node_id',
            strategy='timestamp',
            updated_at='run_started_at',
            invalidate_hard_deletes=true
        )
    }}

with
    
base as (
    select * from {{ ref("tests") }}
),

enhanced as (
    select
        node_id,
        run_started_at,
        name,
        depends_on_nodes,
        package_name,
        test_path,
        tags
    from base
    where
        node_id in 
        {{ get_unique_nodes(type='test') }}
    qualify ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY run_started_at desc) = 1
    )

select *
from enhanced

 {% endsnapshot %}