{% macro set_query_tag() -%}

{% set original_query_tag = get_current_query_tag() %}
{% set new_query_tag = config.get('query_tag') %}

  {% if new_query_tag %}
  
    {% set original_query_tag = get_current_query_tag() %}
    {{ log("Setting query_tag to '" ~ new_query_tag ~ "'. Will reset to '" ~ original_query_tag ~ "' after materialization.") }}
    {% do run_query("alter session set query_tag = '{}'".format(new_query_tag)) %}
    {{ return(original_query_tag)}}
  
  {% else %}
  
    {% set new_query_tag = '{{ "query_tag_type" : "DBT_MODEL", "database" : "{}", "model_name" : "{}", "package" : "{}", "schema" : "{}" , "target" : "{}","role" : "{}","dbt_inovacation_id" : "{}","run_started_at" : "{}"}}'.format(
        model.database,
        model.name,
        model.package_name,
        model.schema,
        target.name,
        target.role,
        invocation_id,
        run_started_at.astimezone(modules.pytz.timezone("America/New_York"))
    ) %}

    {{ log("Setting query_tag to " + new_query_tag) }}
    {% do run_query("alter session set query_tag = '{}'".format(new_query_tag)) %}
    {{ return(original_query_tag) }}
  {% endif %}

{% endmacro %}