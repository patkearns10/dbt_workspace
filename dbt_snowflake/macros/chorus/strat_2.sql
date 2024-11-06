{% macro snapshot_timestamp_strategy2(node, snapshotted_rel, current_rel, config, target_exists,source_columns) %}
    {% set primary_key = config['unique_key'] %}
    {% set updated_at = config['updated_at'] %}
    {% set invalidate_hard_deletes = config.get('invalidate_hard_deletes', false) %}

    {#/*
        The snapshot relation might not have an {{ updated_at }} value if the
        snapshot strategy is changed from `check` to `timestamp`. We
        should use a dbt-created column for the comparison in the snapshot
        table instead of assuming that the user-supplied {{ updated_at }}
        will be present in the historical data.

        See https://github.com/dbt-labs/dbt-core/issues/2350
    */ #}
    {% set row_changed_expr -%}
        -- ({{ snapshotted_rel }}.dbt_valid_from < {{ current_rel }}.{{ updated_at }})
        ({{ snapshotted_rel }}.effective_from < {{ current_rel }}.{{ updated_at }})
    {%- endset %}

    {% set scd_id_expr = snapshot_hash_arguments([primary_key, updated_at]) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr,
        "invalidate_hard_deletes": invalidate_hard_deletes,
        "custom_sk_col_name": custom_sk_col_name,
        "custom_sk_seq_name": custom_sk_seq_name
    }) %}
{% endmacro %}



{% macro snapshot_check_strategy2(node, snapshotted_rel, current_rel, config, target_exists,source_columns) %}
    {% set check_cols_config = config['check_cols'] %}
    {% set primary_key = config['unique_key'] %}
    {% set invalidate_hard_deletes = config.get('invalidate_hard_deletes', false) %}
    {% set updated_at = config.get('updated_at', snapshot_get_time()) %}
    {% set custom_sk_col_name = config.get('custom_surrogate_key')["column"] %}
    {% set custom_sk_seq_name = config.get('custom_surrogate_key')["sequence"] %}
    {% set custom_create_record_on_delete = config.get('custom_create_record_on_delete', false) %}
    {% set overwrite_cols_cols_config = config.get('overwrite_cols',[]) %}
    {% set from_timestamp = config.get('custom_etl_at_ts',snapshot_get_time())%}
    {% set raw_target_predicates = config.get('target_predicates',[])%}
    {% set custom_source_has_delete_flag = config.get('custom_source_has_delete_flag', false) %}





    {% set log_mode = 'none' %}            

    {% set log_formatted_config %}
    {% for key,value in config.items() %}
        {{ key }}: {{ value }}
    {%- endfor %}

    {% endset %}

    {{ log("Logging formatted config: \n" ~  log_formatted_config  ) }} 
    {{ log("Logging raw_target_predicates: \n" ~  raw_target_predicates  ) }} 


    
    {%- set target_predicates = [] %}
    {% if execute %} 
        {%- for predicate in raw_target_predicates %}
            {%- do target_predicates.append(predicate) %}
        {% endfor %}


    {% endif %}
    
    {{ log("target_predicates: " ~  target_predicates ) }} 


    {% if overwrite_cols_cols_config|length == 0  %}
        {{ log("overwrite_cols_cols_config (overwrite on change): not configured") }}
    {% else %}        
        {{ log(" overwrite_cols_cols_config (overwrite on change): " ~  overwrite_cols_cols_config ) }}    
    {% endif %}

    {# detect on both check and overwrite columns #}
    {% set change_cols = check_cols_config + overwrite_cols_cols_config  %}
   
    {% set column_added = false %}

    {# check if a column is added for add row. if true then the query can be built differently 
    will error if a check_col is not in node sql
    #}
    {% set column_added, check_cols = snapshot_check_all_get_existing_columns(node, target_exists, check_cols_config) %}

    {{ log("Logging check_cols: " ~  check_cols ~ " contains new column:" ~ column_added ) }}

    {#get the columns from the query using where false limit 0#}
    {%- set query_columns = get_columns_in_query(node['compiled_code']) -%}

    {#get the columns that cant be compared using = !=, could be better handling here#}
    {% set hash_check_source_columns = [] %}
      {% for column in source_columns %}
        {#get geography and geometry for separate hanlding. use hash until snowflake functions improve#}
        {% if column.dtype in  ('GEOGRAPHY','GEOMETRY') %}
            {% do hash_check_source_columns.append(adapter.quote(column.name)) %}
        {% endif %}
      {% endfor %}

    {#get the columns that can be compared using = #}
    {% set change_cols_simple = [] -%}
        {% for item in change_cols if item not in hash_check_source_columns and  adapter.quote(item)|upper  not in hash_check_source_columns -%}
        {% do change_cols_simple.append(item) -%}
    {% endfor -%}


    {%- set row_changed_expr -%}
    (
        {%- if column_added -%}
                                                                                                                                    
            {{ get_true_sql() }}
        {%- else -%}
                                    
            {%- for col in change_cols -%}
                {% if col in change_cols_simple -%}
                    {{ snapshotted_rel }}.{{ col }} != {{ current_rel }}.{{ col }}
                    or ({{ snapshotted_rel }}.{{ col }} is null and {{ current_rel }}.{{ col }} is not null)
                    or ({{ snapshotted_rel }}.{{ col }} is not null and {{ current_rel }}.{{ col }} is null)
                {% else -%}
                    hash({{ snapshotted_rel }}.{{ col }}) != hash({{ current_rel }}.{{ col }})
                {% endif -%}
                {%- if not loop.last -%}
                    or {% endif -%}
            {%- endfor -%}
        {%- endif -%}
    )
    {%- endset %}

    {%- set row_changed_overwrite_expr -%}
    (
    {%- if column_added -%}
    {#---review--- If a column is added to check_cols then no need to do change detection as all records would be added, so just set to TRUE#}
        {{ get_true_sql() }}
    {%- else -%}
    {#%- for col in check_cols -%#}
    {%- for col in check_cols_config -%}
        {% if col in change_cols_simple -%}    
        ({{ snapshotted_rel }}.{{ col }} = {{ current_rel }}.{{ col }} or (({{ snapshotted_rel }}.{{ col }} is null) and ({{ current_rel }}.{{ col }} is null)))
        {%else -%}        
        hash({{ snapshotted_rel }}.{{ col }}) = hash({{ current_rel }}.{{ col }}) 
        {% endif -%}        
        {%- if not loop.last -%} and {% endif -%}
    {%- endfor -%}
    {%- endif -%}
    )
    {%- endset %}

    {% set scd_id_expr = snapshot_hash_arguments([primary_key, from_timestamp]) %}
    {% if log_mode == 'debug' %}
        {{ log("---------Logging row_changed_expr:\n\n" ~  row_changed_expr  ~ "\n\n" ) }}
        {{ log("---------Logging row_changed_overwrite_expr:\n\n" ~  row_changed_overwrite_expr  ~ "\n\n") }}
        {{ log("---------Logging scd_id_expr:\n\n" ~  scd_id_expr  ~ "\n\n") }}
    {% endif %}

    
    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": scd_id_expr,
        "invalidate_hard_deletes": invalidate_hard_deletes,
        "custom_sk_col_name": custom_sk_col_name,
        "custom_sk_seq_name": custom_sk_seq_name,
        "custom_create_record_on_delete": custom_create_record_on_delete,
        "row_changed_overwrite_expr" : row_changed_overwrite_expr,
        "overwrite_cols_cols_config" : overwrite_cols_cols_config,
        "from_timestamp" : from_timestamp,
        "target_predicates" : target_predicates,
        "custom_source_has_delete_flag" : custom_source_has_delete_flag
    }) %}
{% endmacro %}


{% macro snapshot_get_time2() -%}
to_timestamp_ntz(convert_timezone('Pacific/Auckland', current_timestamp()))
{%- endmacro %}