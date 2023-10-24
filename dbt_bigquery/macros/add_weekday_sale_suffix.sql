{% macro add_weekday_sale_suffix(table_name='default') -%}
{%- set weekday = (var("weekday", var("weekday_sale_default_weekday")) | trim | lower) -%}
{%- set suffix = (var("weekday_sale") | selectattr("weekday", "eq", weekday) | list | last)["weekday"] -%}
{{ table_name }}_{{ suffix }}
{%- endmacro %}