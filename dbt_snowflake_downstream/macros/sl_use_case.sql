{% macro sl_use_case(use_case=1) %}
  {% if env_var('DBT_SL_USE_CASE', 1) | int >= use_case %}
    {% do return(True) %}
  {% else %}
    {% do return(False) %}
  {% endif %}
{% endmacro %}