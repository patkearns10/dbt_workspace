{% macro did_tests_run(results) %}
  {% set count = namespace(value=0) %}
  {%- for result in results if result.node.resource_type == 'test' and result.status != 'skipped' -%}
    {% set count.value = count.value + 1 %}
  {%- endfor -%}

  {{ log('$$$ Number of failed tests: ' ~ count.value, info=true) }}

  {% if count.value == 0 -%}
    {{ log("found no test results to process.") if execute }}
    {{ return('') }}
  {% else %}
    {{ log("found " ~ count.value ~ "test results to process.") if execute }}
    -- replace below with your logic
    select 1 as col 
  {% endif %}
{% endmacro %}