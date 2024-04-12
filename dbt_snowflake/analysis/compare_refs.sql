{% set old_relation=ref('dim_customers') %}
{% set new_relation=ref('dim_customers__new') %}

-- {{ old_relation }}
-- {{ new_relation }}

{{ audit_helper.compare_relations(
    a_relation=old_relation,
    b_relation=new_relation,
    primary_key="customer_id"
) }}