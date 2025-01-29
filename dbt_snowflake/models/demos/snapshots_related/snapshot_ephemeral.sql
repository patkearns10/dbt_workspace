{{
    config(
        materialized='ephemeral'
    )
}}

with
    
base as (
    select * from {{ ref("models") }}
),

enhanced as (
    select
        node_id,
        run_started_at,
        database,
        schema,
        name,
        depends_on_nodes,
        package_name,
        path,
        checksum,
        materialization,
        tags,
        meta,
        alias,
        dbt_cloud_environment_name,
        dbt_cloud_environment_type
    from base
    where
        node_id in 
        {{ get_unique_nodes(type='model') }}
    qualify ROW_NUMBER() OVER (PARTITION BY node_id, dbt_cloud_environment_name, dbt_cloud_environment_type ORDER BY run_started_at desc) = 1
    )

select *
from enhanced