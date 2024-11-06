{% macro get_mod_ref(mod_ref) %}

{{ log("Logging mod_ref : " ~  mod_ref  ) }}
{%- set my_object_str %}
        '{{ref(mod_ref)}}'
    {%endset%}
{{ log("Logging my_object_str : " ~  my_object_str  ) }}
{{return(my_object_str)}}

{% endmacro %}


 {% macro get_model(relation) -%}
    {% for node in graph.nodes.values()
        | selectattr("resource_type", "equalto", "model")
        | selectattr("name", "equalto", relation.identifier) %}
        {% do return(node) %}
    {% endfor %}
{%- endmacro %}


{% macro resolve_str_with_cust_ref(string_input) %}

    {# Regular expression - case insensitive looks for cust_ref with either type of quotes and spaces #}
    {% set match_string = '(?is)(cust_ref\\s*\\(\\s*("|'~ "'" ~ ')(.*?)("|' ~ "')\\s*\\))" %}

    {{ log ("match_string " ~ match_string) }}

    {% set matches = modules.re.finditer(match_string,string_input,flags=0) %}

    {# use namespace to handle scope #}
    {% set ns = namespace(new_string=string_input) %}

    {% for it in matches %}

        {# log ("new_string iterator group(0): " ~ it.group(0)) #} 
        {# log ("new_string iterator group (3): " ~ it.group(3)) #} 
        
        {# replace in the original string, for each full match group(0) the resoved reference from group(3) #}
        {% set ns.new_string = ns.new_string.replace(it.group(0)|string, ref(it.group(3))|string) %}

    {% endfor %}

    {{ log ("new_string to return with resolved ref: " ~ ns.new_string) }} 
    {# Return the modified string #}
    {{ return(ns.new_string) }}

{% endmacro %}