{#
    This macro generates a dictionary object called "workload_access_map"
    This object captures all the information required in order to run grant access required for all the exposures defined in the project
#}

{% macro generate_workload_access_map() %}
    {% set workload_access_map = {} %}

    {# Loop through exposures and their depends_on workload_access_map #}
    {% for exposure_name in graph.exposures %}
        {% set exposure = graph.exposures[exposure_name] %}

        {% for depends_on_node in exposure.depends_on.nodes %}

            {# Check the depends_on node is a valid model in the manifest #}
            {% if depends_on_node in graph.nodes %} 
                
                {% set depends_on_node_name = graph.nodes[depends_on_node]['name'] %}
                {% set exposure_roles = exposure.meta.workload_access.aws %}
                {% set exposure_grantee = exposure.meta.workload_access.data_product %}
                {% set role_map = {}%}
            
                -- If the depends_on node doesn't exist in the access map yet
                {% if depends_on_node_name not in workload_access_map %}
                    {% for role in exposure_roles %}
                        {% do role_map.update({ generate_principal_set_name(role): generate_grantee_name(exposure_grantee) }) %}
                    {% endfor %}

                    {% set workload_access_map = workload_access_map.update({
                            depends_on_node_name: {
                                'existing_grants_config': graph.nodes[depends_on_node]['config']['grants'],
                                'relation_name': graph.nodes[depends_on_node]['relation_name'],
                                'relation_database': graph.nodes[depends_on_node]['database'],
                                'relation_schema': graph.nodes[depends_on_node]['schema'],
                                'relation_object_name': graph.nodes[depends_on_node]['name'],
                                'exposure_role_map': role_map
                            },
                        }) 
                    %}

                -- If the model already exists as a dependency to a different exposure 
                {% else %}
                     {% for role in exposure_roles %}
                     
                        -- If the role isn't already being granted
                        {% if role not in workload_access_map[depends_on_node_name].exposure_role_map %}
                            {% do workload_access_map[depends_on_node_name].exposure_role_map.update(
                                    { generate_principal_set_name(role): generate_grantee_name(exposure_grantee) }
                                )
                            %}

                        -- If the role is already being granted to certain grantees, we just want to add to this list
                        {% else %}
                            {% set updated_grantee_list = workload_access_map[depends_on_node_name].exposure_role_map[role] + exposure_grantee %}
                            {% do workload_access_map[depends_on_node_name].exposure_role_map.update(
                                    { generate_principal_set_name(role): generate_grantee_name(updated_grantee_list) }
                                )
                            %}


                        {% endif %}
                    {% endfor %}
                {% endif %}

            {% endif %}

        {% endfor %}

    {% endfor %}
    {{ log(workload_access_map)}}
    {{ return(workload_access_map) }}

{% endmacro %}

-- Run the grant statements on_run_end
{% macro run_all_exposure_grants() %}
{% set map = generate_workload_access_map() %}

    {% for model_name in map %}
        {% set model = map[model_name] %}

        {% if execute %}
            
            {% set existing_relation = adapter.get_relation(
                database = model.relation_database,
                schema = model.relation_schema,
                identifier = model.relation_object_name
            ) %}

            {{ log('Apply grants ' ~ model.exposure_role_map ~ ' to object ' ~ model.relation_name, info=True )}}
            {{ log('Relation: ' ~ existing_relation, info=True )}}

            {% do apply_grants(
                    relation=existing_relation,
                    grant_config=model.exposure_role_map,
                    should_revoke=True
                )
            %}

        {% endif %}

    {% endfor %}
{% endmacro %}


-- Run the grant statements in a post-hook
{% macro run_model_exposure_grants(model_name) %}

{% set map = generate_workload_access_map() %}

    {% if model_name in map %}
        {% set model = map[model_name] %}

        {% set existing_relation = adapter.get_relation(
            database = model.relation_database,
            schema = model.relation_schema,
            identifier = model.relation_object_name
        ) %}

        {{ log('Apply grants ' ~ model.exposure_role_map ~ ' to object ' ~ model.relation_name, info=True )}}
        {{ log('Relation: ' ~ existing_relation, info=True )}}

        {% do apply_grants(
                relation=existing_relation,
                grant_config=model.exposure_role_map,
                should_revoke=True
            )
        %}
    {% else %}

        {{ log('Exposure configuration not found for model ' ~ model_name ~ ' ,  no grants have been run', info=True )}}

    {% endif %}

{% endmacro %}

{# This macro expects data_product to be a list #}
{% macro generate_grantee_name(data_product) %}
    {% set grantee_list = [] %}
    {% for grantee in data_product %}
        {% if grantee not in grantee_list %}
            {% do grantee_list.append("serviceAccount:mam_" ~ grantee ~ "_<env>@cpt-example-1234.iam.gserviceaccount.com") %}
        {% endif %}
    {% endfor %}
    {{ return(grantee_list) }}

{% endmacro %}

{# This macro expects aws to be a string #}
{% macro generate_principal_set_name(aws) %}
    {{ return("principalSet://iam.googleapis.com/projects/663211149047/locations/global/workloadIdentityPools/mam-aws-wif-non/attribute.aws_role/" ~ aws) }}
{% endmacro %}