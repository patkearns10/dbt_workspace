{#
    Macro which translates a list of AWS arns to a list of users that roles will be granted to.
#}

{% macro generate_aws_user_list(aws) %}
    {% set user_list = [] %}
    {% for arn in aws %}
        {% set user = "user:principalSet://iam.googleapis.com/projects/663211149047/locations/global/workloadIdentityPools/cpt-aws-wif-non/attribute.aws_role/" ~ arn %}
        {% if user not in user_list %}
            {% do user_list.append(user) %}
        {% endif%}
    {% endfor %}
    {{ return(user_list) }}
{% endmacro %}


{#
    Macro which translates a list of data products to a list of service accounts that roles will be granted to.
#}

{% macro generate_data_product_user_list(data_products) %}
    {% set user_list = [] %}
    {% for data_product in data_products %}
        {% set data_product = data_product | replace("_", "-") %}
        {% set service_account = "serviceAccount:dbt-" ~ data_product ~ "@non-mamadpdaasdgenl-7410.iam.gserviceaccount.com" %}
        {% if service_account not in user_list %}
            {% do user_list.append(service_account) %}
        {% endif %}
    {% endfor %}
    {{ return(user_list) }}
{% endmacro %}


{#
    Macro which generates the AGG group name of the customer mart used for grants.
#}

{% macro generate_consumer_mart_group(exposure) %}
    {% if exposure.name == "core" %}
        {{ return([]) }}
    {% endif %}
    {% set platform_env = "" if env_var("DBT_CLOUD_ACCOUNT_ID", 3) == 4 else "n" %}
    {% set dp_code = exposure.package_name | replace("_", "-") %}
    {% set gcp_env = "daasmain" if env_var("DBT_CLOUD_INVOCATION_CONTEXT") == "prod" else "daasstg" %}
    {% set group = "group:agg-mam-daas" ~ platform_env ~ "-" ~ dp_code ~ "-gcp-" ~ gcp_env ~ "-consumer-" ~ exposure.name ~ "-read@macquarie.com" %}
    {{ return([group]) }}
{% endmacro %}

 
{#
    Macro which generates a map of model names to grant configs. An example output is as below:
    {
        orders: {
            relation: <dbt-relation-object>
            grant_config: {
                "roles/bigquery.dataViewer": [
                    "user:first.last@example.com",
                    "group:my-group@example.com",
                    "serviceAccount:robot@example.iam.gserviceaccount.com",
                    ...
                ]
            }
        },
        ...
    }
#}

{% macro generate_model_grant_config_map(model=none) %}
    {# BQ Role that will be applied to all users, groups, and service accounts #}
    {% set role_name = "roles/bigquery.dataViewer" %}
    {% set map = {} %}
    {# Loop through all exposures inside the current dbt project #}
    {% for exposure_key in graph.exposures %}
        {# Fetch the aws arns and data products that need access via this exposure #}
        {% set exposure = graph.exposures[exposure_key] %}
        {% set aws = exposure.meta.workload_access.aws %}
        {% set data_products = exposure.meta.workload_access.data_product %}
        {# Only proceed if there is workload access defined or its a consumer mart #}
        {% if aws or data_products or exposure.name != "core" %}
            {# Loop through all dependencies of the exposure #}
            {% for dependant_node in exposure.depends_on.nodes %}      
                {# Check the dependency has a corresponding dbt model #}
                {% if dependant_node in graph.nodes %}
                    {% set node = graph.nodes[dependant_node] %}                
                    {# Continue if no model has been provided or the model provided matches the dependency #}
                    {% if not model or model.identifier == node.name %}               
                        {# Generate a list of users/groups/service-accounts that need the role granted to them for the current model #}
                        {% set user_list = generate_aws_user_list(aws) + generate_data_product_user_list(data_products) + generate_consumer_mart_group(exposure) %} 
                        {# If no entry in the map exists, get the relation and update the map with the grant config #}
                        
                        {{ log('--$$ node.database: ' ~ node.database, info=True) }}
                        {{ log('--$$ node.schema: ' ~ node.schema, info=True) }}
                        {{ log('--$$ node.name: ' ~ node.name, info=True) }}
                        {{ log('--$$ model: ' ~ model, info=True) }}

                        {% if node.name not in map %}
                            {{ log('--$$ confirm node.name not in map', info=True) }}
                            {% set relation = adapter.get_relation(
                                database = node.database,
                                schema = node.schema,
                                identifier = node.name,
                                )
                            %}

                            {{ log('--$$ get relation: ' ~ relation, info=True) }}

                            {% if relation is none %}
                                {% set materialized_mapping =
                                    {
                                        'incremental': 'table',
                                        'table': 'table',
                                        'view': 'view',
                                        'materialized_view': this.MaterializedView
                                    }
                                %}

                                {%- set relation = api.Relation.create(
                                    database = node.database,
                                    schema = node.schema,
                                    identifier = node.name,
                                    type = materialized_mapping[node.config.materialized],
                                    )
                                -%}

                                {{ log('--$$ relation create: ' ~ relation, info=True) }}

                            {% endif %}


                            {% do map.update({
                                node.name: {
                                    "relation": relation,
                                    "grant_config": {role_name: user_list}
                                }
                            }) %}

                        {# If an entry in the map already exists, update that entry with the new users/groups/service-accounts #}
                        {% else %}
                            {% for user in user_list %}
                                {% if user not in map[node.name].grant_config[role_name] %}
                                    {% do map[node.name].grant_config[role_name].append(user) %}
                                {% endif %}
                            {% endfor%}
                        {% endif %}

                    {# Skip this dependency if it does not match the model name of the current post-hook run #}
                    {% else %}
                        {{ log("Skipping grant for dependency '" ~ dependant_node ~ "' in exposure '" ~ exposure.name ~ "' as it does not match name of current model '" ~ model.identifier ~ "'", info=True) }}
                    {% endif %}

                {# Skip this dependency if an equivalent model does not exist #}
                {% else %}
                    {{ log("Skipping grant for dependency '" ~ dependant_node ~ "' in exposure '" ~ exposure.name ~ "' as the model does not exist", info=True) }}          
                {% endif %}      
            {% endfor %}

         {# Skip this exposure if there are no grants defined #}
        {% else %}       
            {{ log("No grants defined for exposure '" ~ exposure.name ~ "'", info=True) }}
        {% endif %}
    {% endfor %}

    {{ return(map)}}

{% endmacro %}


{#
    Entrypoint macro for the exposure grants which fetches the grant configs for all models
    and applies these grants to the relevant models.
#}

{% macro run_exposure_grants(model=none) %}
    {% set map = generate_model_grant_config_map(model) %}
    {% for node_name in map %}
        {{ log("Applying grants " ~ map[node_name].grant_config ~ " to model '" ~ node_name ~ "'", info=True) }}
        {% do apply_grants(
            relation = map[node_name].relation,
            grant_config = map[node_name].grant_config,
            should_revoke=True
        ) %}
    {% endfor %}
{% endmacro %}


{%- macro bigquery__get_grant_sql(relation, privilege, grantee) -%}
    
    {{ log('--$$ relation: ' ~ relation, info=True) }}
    {{ log('--$$ relation type: ' ~ relation.type, info=True) }}

    {% if relation.type == 'materialized_view' %}
        grant `{{ privilege }}` on materialized view {{ relation }} to {{ '\"' + grantee|join('\", \"') + '\"' }}
    {% else %}
        grant `{{ privilege }}` on {{ relation.type }} {{ relation }} to {{ '\"' + grantee|join('\", \"') + '\"' }}
    {% endif %}

{%- endmacro -%}


{%- macro bigquery__get_revoke_sql(relation, privilege, grantee) -%}
    {{ log('--$$ relation: ' ~ relation, info=True) }}
    {{ log('--$$ relation type: ' ~ relation.type, info=True) }}

    {% if relation.type == 'materialized_view' %}
        revoke `{{ privilege }}` on materialized view {{ relation }} from {{ '\"' + grantee|join('\", \"') + '\"' }}
    {% else %}
        revoke `{{ privilege }}` on {{ relation.type }} {{ relation }} from {{ '\"' + grantee|join('\", \"') + '\"' }}
    {% endif %}
    
{%- endmacro -%}