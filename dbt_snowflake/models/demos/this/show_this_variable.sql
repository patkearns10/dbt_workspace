{{
    config(
        alias='an_aliased_table',
        materialized='table',
        tags=["some_tag"]
    )
}}



select 
    'hello, world!' as test_col,
    -- get columns from another table
    {{ dbt_utils.get_filtered_columns_in_relation(from=ref('my_first_model')) }} as utils_get_filtered_columns,
    -- get name of this table
    '{{ this }}' as this,
    -- with alias it will provide alias
    '{{ this.name }}' as this_name,
    -- config info
    '{{ model.config.alias }}' as model_alias,
    {{ model.config.tags }} as model_tags,
    '{{ model.config.materialized }}' as model_materialization,
    -- model info
    '{{ model.alias }}' as model_alias_v2,
    '{{ model.name }}' as model_name,
    '{{ model.database }}' as model_database,
    '{{ model.schema }}' as model_schema,
    {{ model.fqn }} as model_fully_qualified_name,
    '{{ model.unique_id }}' as model_unique_id,
    '{{ model.path }}' as model_path
