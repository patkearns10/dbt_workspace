{% set obj=adapter.get_relation(
      database=target.database,
      schema="DBT_PKEARNS",
      identifier="CUSTOMERS"
) -%}

-- _is_relation tests
{{ obj }}
{{ obj is mapping }}
{{ obj.get('metadata', {}).get('type', '').endswith('Relation') }}

-- # we expect this to be true and true, so pass.
    {% if not (obj is mapping and obj.get('metadata', {}).get('type', '').endswith('Relation')) %}
        'pass'
    {% else %}
        'fail'
    {% endif %}


-- # storage
{#
{% do exceptions.raise_compiler_error("we got here: " ~ obj.get('metadata', {}).get('type', '')) %}
#}
