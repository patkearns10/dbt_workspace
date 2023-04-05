{# a user-friendly interface into statements #}
{% macro run_query(sql) %}
    {% call statement("run_query_statement", fetch_result=true, auto_begin=false) %}
        {{ sql }}
    {% endcall %}


    {% set results = load_result("run_query_statement").table %}
    {% do log('DEBUG===============START=================DEBUG', info=True) %}
    -----------------------------------
    {% set ns = namespace(column_chars=0, row_chars=0, total_chars=0) -%}
    {% for column in results.column_names -%}
    {%- set ns.column_chars = ns.column_chars + (column | length) -%}
    {% endfor %}
    {% do log('columns = ' ~ results.columns | length, info=True) %}
    {% do log('column_chars = ' ~ ns.column_chars, info=True) %}

    {% for item in range(results.columns | length) -%}
        {% for word in results.columns[item].values() | list -%}
                {%- set ns.row_chars = ns.row_chars + (word | length) -%}
        {% endfor -%}
    {% endfor %}
    {% do log('rows = ' ~ results.rows | length, info=True) %}
    {% do log('row_chars = ' ~ ns.row_chars, info=True) %}
    {% do log('total_chars = ' ~ ns.column_chars + ns.row_chars, info=True) %}

    {% do log('DEBUG===============END=================DEBUG', info=True) %}
    {% do return(results) %}
  
{% endmacro %}