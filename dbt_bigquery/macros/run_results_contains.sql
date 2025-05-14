{% macro run_results_contains(results) %}
  {% for item in results %}
     {{ item.to_dict() | tojson(indent=4) }}
     -----------------------
  {% endfor %}
{% endmacro %}