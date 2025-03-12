{# drop_by_subdirectory

This macro drops all the models within a selected database, schema, directory, and prefix to "clean" the workspace. Use the dry_run param to see the schemas that will be dropped before dropping them.

Args:
    - database: string      -- the name of the database to clean. By default the target.database is used
    - dry_run: bool         -- dry run flag. When dry_run is true, the cleanup commands are printed to stdout rather than executed. This is true by default
    - schema: string        -- case-insensitive schema to include. This is None by default. 
    - directory: string     -- case-sensitive name of directory or subdirectory to include. This is None by default 
    - prefix: string        -- case-sensitive like pattern of prefix of node name to include. This is None by default 

Example 1 - dry run of current database
    dbt run-operation drop_by_subdirectory    
    
Example 2 - dry run of drop any nodes in a given database that match a given directory string
    dbt run-operation drop_by_subdirectory --args '{database: my_database, dry_run: False, directory: "marts" }'

Example 3 - dry run of drop any nodes in default database that match a given directory and prefix string
    dbt run-operation drop_by_subdirectory --args '{"directory": "marts", "prefix": "stg"}'
     
Example 4 - drop any nodes in default database that match a given directory and prefix string
    dbt run-operation drop_by_subdirectory --args '{"directory": "marts", "prefix": "stg", "dry_run": "False"}'

Example 5 - full clean :)
    dbt run-operation drop_by_subdirectory --args '{dry_run: False}'
#}

{% macro drop_by_subdirectory(database=target.database, schema=None, dry_run=True, directory=None, prefix=None) %}
    {%- set msg -%}
        Starting clean_workspace...
          database:     {{ database }}
          schema:       {{ schema }}
          dry_run:      {{ dry_run }} 
          directory:    {{ directory }} 
          prefix:       {{ prefix }} 
    {%- endset -%}
    {{ log(msg, info=True) }}

    {% set models_of_interest = get_models(directory=directory, prefix=prefix) %}
    {{ log('Nodes to drop: ' ~ models_of_interest, info=True) }}
    
    {% set cleanup_query %}
        with 
        models_to_drop as (
            select
                case
                    when table_type = 'BASE TABLE' then 'TABLE'
                    when table_type = 'VIEW' then 'VIEW'
                end as relation_type,
                concat_ws('.', table_catalog, table_schema, table_name) as relation_name
            from
                {{ target.database }}.information_schema.tables
            where
                table_name in
                    (
                        {%- for model in models_of_interest -%}
                            '{{ model.upper() }}'
                            {%- if not loop.last -%},{% endif %}
                        {%- endfor -%}
                    )
                {% if schema %}
                    and lower(table_schema) = lower('{{ schema }}')
                {% endif %}
        )

        select
            'drop ' || relation_type || ' ' || relation_name || ';' as drop_commands
        from models_to_drop
        -- intentionally exclude unhandled table_types, including 'external table`
        where drop_commands is not null
    {% endset %}
                    
    {{ log('\nGenerating cleanup queries...\n', info=True) }}
    {% set drop_queries = run_query(cleanup_query).columns[0].values() %}


    {% for drop_query in drop_queries %}
        {% if execute and not dry_run %}
            {{ log('Dropping schema with command: ' ~ drop_query, info=True) }}
            {% do run_query(drop_query) %}    
        {% else %}
            {{ log(drop_query, info=True) }}
        {% endif %}
    {% endfor %}
{% endmacro %}