{#
  Custom materialization: incremental_direct_insert (Redshift-only)

  Use case: the model's compiled SQL already selects exactly the rows that
  should land in the target (e.g. it reads a staging table that is
  truncate-and-loaded with one day of data upstream). There is no need for
  is_incremental()/{{ this }} filtering, and no need for dbt to build a
  temp table just so a merge/append macro can read it back.

  Behavior:
    - Relation doesn't exist yet, or --full-refresh: create table as select
      (normal CTAS, no temp table involved).
    - Relation exists, not full-refresh:
        optional: delete rows from target matching a predicate (e.g. the
          same load_date that's about to be re-inserted, for idempotent
          reruns)
        insert into target (columns) select (columns) from (<compiled sql>)
      -- straight from the model's own query result set into target, one
      statement, no intermediate physical table.
#}

{% materialization incremental_direct_insert, adapter='redshift' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set full_refresh_mode = (should_full_refresh() or existing_relation is none or existing_relation.is_view) -%}

  {#- optional: config(incremental_predicates=["load_date = current_date"]) to
      delete the slice you're about to reinsert, for idempotent reruns -#}
  {%- set incremental_predicates = config.get('incremental_predicates', none) or config.get('delete_predicates', none) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here on Redshift:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}

  {% if full_refresh_mode %}

    {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}

  {% else %}

    {%- set dest_columns = adapter.get_columns_in_relation(existing_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

    {% set build_sql %}
      {% if incremental_predicates %}
        delete from {{ target_relation }}
        where ({{ incremental_predicates | join(') and (') }});
      {% endif %}

      insert into {{ target_relation }} ({{ dest_cols_csv }})
      select {{ dest_cols_csv }}
      from (
        {{ sql }}
      ) as model_subquery;
    {% endset %}

  {% endif %}

  {% call statement("main") %}
    {{ build_sql }}
  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% for rel in to_drop %}
    {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
