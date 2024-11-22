{{
    config(
        materialized='table'
    )
}}

with 
source_models as (
    select
        node_id,
        name as model_name,
        database, 
        schema
    from 
        {{ ref('dbt_artifacts_models') }}
    where dbt_valid_to is null

    union all
    
    select
        node_id,
        name as model_name,
        database, 
        schema
    from {{ ref('dbt_artifacts_sources') }}
    where dbt_valid_to is null
),
    
source_tests as (
    select
        node_id,
        name as test_name,
        test_path
    from {{ ref('dbt_artifacts_tests') }}
    where dbt_valid_to is null
),

test_executions as (
    select 
        command_invocation_id,
        node_id,
        run_started_at,
        status,
        failures
    from {{ ref('dbt_artifacts', 'test_executions') }}
    qualify row_number() over (partition by node_id order by run_started_at desc) = 1
),

first_failed_at as (
    select distinct
        command_invocation_id,
        node_id,
        min(run_started_at) over (partition by node_id) as first_failed_at,
        status,
        failures
    from test_executions
    where status in ('error','warn')

)

select * from first_failed_at


-- TODO:
-- Need to add more logic to tests to capture 

-- test_severity_config,
-- model_refs,
-- source_refs,
-- column_names,
-- test_type,



-- model_test as (

--     select 
--         source_models.model_name,
--         source_models.node_id,
--         source_models.database,
--         source_models.schema,
--         source_tests.test_name,
--         exec.node_id as execution_node_id,
--         tests.test_node_id as test_node_id,
--         exec.status as status,
--         exec.failures as failures,
--         exec.latest_run as latest_run,
--         failed.first_failed_at as first_failed
--     from source_models
--     left join source_tests
--         on source_models.node_id = source_tests.model_node
--     left join test_executions as exec
--     on tests.test_node_id = exec.execution_node_id
--     left join first_failed_at as failed
--     on exec.command_invocation_id = failed.command_invocation_id
--     and exec.execution_node_id = failed.node_id
-- )

-- select 
--     model_name,
--     model_node_id,
--     database,
--     schema,
--     category,
--     priority,
--     test_name,
--     execution_node_id,
--     test_node_id,
--     status,
--     failures,
--     latest_run,
--     first_failed
-- from 
--     model_test