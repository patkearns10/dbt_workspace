{% macro test_results(results) %}
    {%- set test_results = [] -%}

    {%- for result in results if result.node.resource_type != 'test' -%}
        {{ result }}
        ---=============
    {% endfor %}
    {%- for result in results if result.node.resource_type == 'test' -%}
        {%- set test_results = test_results.append(result) -%}
    {%- endfor -%}
        {%- for test in test_results %}
            --------------

            {{ test.to_dict() }}
            ----------------------------
            {{ test.to_dict().node.get('file_key_name') }}
            {{ test.to_dict().node.depends_on.get('file_key_name') }}

        {%- endfor -%}
{% endmacro %}