{# a user-friendly interface into statements #}
{% macro run_query(sql) %}
    {% call statement("run_query_statement", fetch_result=true, auto_begin=false) %}
        {{ sql }}
    {% endcall %}

    {% do log('DEBUG================================DEBUG', info=True) %}
    {% do log("DEBUG - HOW MANY ITEMS in run_query(): " ~ load_result("run_query_statement").table | length, info=True) %}
    {% do return(load_result("run_query_statement").table) %}
  
{% endmacro %}