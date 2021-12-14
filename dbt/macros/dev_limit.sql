{% macro dev_limit(limit=1000) %}

{#-
    This macro is used to add a row limit in CI checks so that they run much faster
    In Cloud we set the env_var DBT_ENVIRONMENT_NAME equal to 'prod' for all job runs and development, 
        except for the Slim CI job, which overrides it to 'ci'. which will trigger this `if` block
    In code, we default the env_var to 'dev', (which does not trigger the `if` block), but will be useful
        to avoid compilation errors when the environment variable is not available, for example, in CLI
    We default limit the amount of rows returned to 1000, but you can override this with an argument

    usage: 
        ```sql
            select * from dim_orders
            {{ dev_limit() }}
        ```
    usage:  
        ```sql
            select * from dim_orders
            {{ dev_limit(99) }}
        ```
-#}

  {% if env_var('DBT_ENVIRONMENT_NAME', 'dev') == 'ci' -%}

    limit {{ limit }}
  
  {%- endif %}

{% endmacro %}
