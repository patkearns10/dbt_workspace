with
    base as (select * from {{ ref("dbt_artifacts_models") }}),
    model_executions as (select * from {{ ref("model_executions") }}),
    latest_models as (

        /* Retrieves the models present in the most recent run */
        select * from base where dbt_valid_to is null

    ),

    latest_models_runs as (

        /* Retreives all successful run information for the models present in the most
        recent run and ranks them based on query completion time */
        select
            model_executions.node_id,
            model_executions.was_full_refresh,
            model_executions.query_completed_at,
            model_executions.total_node_runtime,
            model_executions.rows_affected,
            /* Row number by refresh and node ID */
            row_number() over (
                partition by latest_models.node_id, model_executions.was_full_refresh
                order by model_executions.query_completed_at desc  /* most recent ranked first */
            ) as run_idx,
            /* Row number by node ID */
            row_number() over (
                partition by latest_models.node_id order by model_executions.query_completed_at desc  /* most recent ranked first */
            ) as run_idx_id_only
        from model_executions
        inner join latest_models on model_executions.node_id = latest_models.node_id
        where model_executions.status = 'success'

    ),

    latest_model_stats as (

        select
            node_id,
            max(
                case
                    when was_full_refresh
                    then query_completed_at
                end
            ) as last_full_refresh_run_completed_at,
            max(
                case
                    when was_full_refresh
                    then total_node_runtime
                end
            ) as last_full_refresh_run_total_runtime,
            max(
                case
                    when was_full_refresh
                    then rows_affected
                end
            ) as last_full_refresh_run_rows_affected,
            max(case when run_idx_id_only = 1 then query_completed_at end) as last_run_completed_at,
            max(
                case when run_idx_id_only = 1 then total_node_runtime end
            ) as last_run_total_runtime,
            max(case when run_idx_id_only = 1 then rows_affected end) as last_run_rows_affected,
            max(
                case
                    when not was_full_refresh
                    then query_completed_at
                end
            ) as last_incremental_run_completed_at,
            max(
                case
                    when not was_full_refresh
                    then total_node_runtime
                end
            ) as last_incremental_run_total_runtime,
            max(
                case
                    when not was_full_refresh
                    then rows_affected
                end
            ) as last_incremental_run_rows_affected
        from latest_models_runs
        where run_idx = 1
        group by node_id

    ),

    final as (

        select
            latest_models.*,
            latest_model_stats.last_full_refresh_run_completed_at,
            latest_model_stats.last_full_refresh_run_total_runtime,
            latest_model_stats.last_full_refresh_run_rows_affected,
            latest_model_stats.last_run_completed_at,
            latest_model_stats.last_run_total_runtime,
            latest_model_stats.last_run_rows_affected,
            latest_model_stats.last_incremental_run_completed_at,
            latest_model_stats.last_incremental_run_total_runtime,
            latest_model_stats.last_incremental_run_rows_affected
        from latest_models
        left join latest_model_stats on latest_models.node_id = latest_model_stats.node_id

    )

select *
from final

