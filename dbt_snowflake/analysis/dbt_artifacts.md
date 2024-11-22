{% snapshot snapshot_name %}
    {{
        config(
            target_database='development',
            target_schema='dbt_pkearns_snapshots',
            unique_key='node_id',
            strategy='timestamp',
            updated_at='run_started_at'    
        )
    }}

with
    base as (select * from {{ ref("models") }}),
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
            alias
        from base

    )

select *
from enhanced


 {% endsnapshot %}

 ----


{% set objects = graph.nodes.values() | selectattr("resource_type", "equalto", "model") | list %}
{{ objects | tojson(indent=4)}}


-----

select * from {{ ref('models') }}
where name = 'stg_orders'
-- 961d8a09580459d913251eff8e46b274ffdaa976f36be4b06d65985a7d26de3a


-- select name, checksum, count(*) from {{ ref('models') }}
-- group by 1,2

-- select count(*) from {{ ref('models') }}
-- 10640

----

with
    base as (select * from {{ ref("models") }}),
    enhanced as (

        select
            node_id,
            checksum,
            database,
            schema,
            name,
            depends_on_nodes,
            package_name,
            path,
            materialization,
            tags,
            meta,
            alias,
            min(run_started_at) as valid_from,
            max(run_started_at) as valid_to,
        from base
        group by 1,2,3,4,5,6,7,8,9,10,11,12
    )

select *
from enhanced
where name = 'foo_table'


----

select * from {{ ref('models') }} where checksum not in (select checksum from {{ ref('models') }} where checksum = '961d8a09580459d913251eff8e46b274ffdaa976f36be4b06d65985a7d26de3a')

----


    -- SELECT c1, c2 FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS v1 (c1, c2)
    -- where v1.c1 != 1
    -- -- where not in checksum from the target table


        SELECT c1, c2 FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS v1 (c1, c2)
    where v1.c1 not in (select col from {{ ref('sample_ids') }})
-- where checksum not in latest record for the snapshot (valid_from is not null)
-- https://github.com/brooklyn-data/dbt_artifacts/blob/main/macros/upload_results/insert_into_metadata_table.sql
-- add this filter here to remove dupes from coming into source data
-- remove staging dims
-- add staging snapshots
    -- unique_key='node_id',
    -- strategy='check',
    -- check_cols='checksum',

-- make my own fork of dbt_artifacts, install as a package using revision, so I can see what I changed.

-- TODO: log out the insert statement to see why it is throwing an error.