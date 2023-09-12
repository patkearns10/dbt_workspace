{% macro governance_check() %}
   {{ log('Validating object: ' ~ this.identifier, info=True) }}
  
   {% if 'staging' in model.path %}
     {% if not model.name.startswith('stg_') %}
       {% do exceptions.raise_compiler_error('Invalid naming convention. Staging object must start with "stg_": ' ~ model.path) %}
     {% endif %}
   {% endif %}
{% endmacro %}