{% macro xref(package_name, model_name=none, version=none) %}
{#-
    xref() — environment-aware cross-project ref for multi-pre-prod setups.

    Problem this solves (Macquarie demo):
      dbt's native cross-project ref() already resolves based on environment
      type: a downstream PROD run reads the upstream project's PROD relation,
      and any non-prod downstream run (dev, staging, etc.) reads the upstream
      project's STAGING relation, if one is configured. Two gaps that leaves:
        1. dbt Cloud only supports a single non-prod "staging" environment per
           project. Orgs running several distinct long-lived pre-prod
           environments (dev/qa/uat/staging) that are meant to mirror their
           upstream counterparts 1:1 can't do so natively — every downstream
           non-prod run collapses onto that one upstream staging environment
           (or resolves to nothing, if upstream has no staging env configured).
        2. Some orgs want the opposite override for a specific non-prod
           environment: instead of reading upstream's staging (which may have
           stale/untrustworthy data), a chosen non-prod env — e.g. staging —
           should deliberately read the upstream *prod* relation.
      xref() lets either case be handled explicitly, per environment, via
      vars/env vars, without touching dbt's built-in ref() resolution logic
      for everyone else.

    How it works:
      1. If called with a single arg, behaves exactly like ref(model_name) —
         no cross-project ref involved, nothing to override.
      2. If called with (package_name, model_name), it first resolves the
         relation the normal way via builtins.ref(), then — only when the
         `xref_upstream_env` var/env var is explicitly set — rewrites the
         database/schema of that relation to point at the requested upstream
         environment, using a lookup table (`xref_env_map`) rather than any
         hardcoded string-slicing logic. This keeps the redirect opt-in per
         target (e.g. only turn it on for the `staging` job) and keeps the
         env -> database/schema mapping explicit and easy to audit.
      3. Optionally also swaps which *identifier* is read. Macquarie's second
         requirement is that some upstream prod models are explicitly
         published for consumption outside of prod as a `view` (e.g. masked /
         filtered), while prod itself keeps reading the underlying `table`.
         Rather than hardcoding a tag vs. prefix convention, this macro takes
         an explicit map (`xref_exposed_models`) from "package.model" to the
         identifier that should be substituted for non-prod consumers. This
         works whether the upstream team's convention is a tag, a naming
         prefix (e.g. `vw_orders`), or anything else — the map is the single
         source of truth and is easy to point at either convention.

    Vars (set in dbt_project.yml, or via --vars / env_var per job/target):

      xref_upstream_env:
        The upstream environment this run should read from, e.g. 'prod'.
        Leave unset (default '') to fall back to normal dbt Mesh behavior
        (prod->prod, non-prod->upstream staging, no rewrite) — this makes
        xref() a safe drop-in replacement for ref() everywhere else.
        Example: --vars '{"xref_upstream_env": "prod"}'
        or in dbt_project.yml under a target-specific block, or
        env_var('DBT_XREF_UPSTREAM_ENV').

      xref_env_map:
        Dict mapping an upstream env name to the database/schema to redirect
        to, e.g.:
          xref_env_map:
            prod:
              database: ANALYTICS_PROD
              # schema: (optional — omit to keep the schema ref() resolved)

      xref_exposed_models:
        Dict mapping "package_name.model_name" -> the identifier (table/view
        name) that should be substituted when redirecting, e.g.:
          xref_exposed_models:
            secret.orders: vw_orders_prod_exposed

    Usage: identical to ref() — {{ xref('secret', 'orders') }} or
    {{ xref('my_model') }} for a same-project ref.
-#}

{%- if model_name is none -%}
    {%- set model_name = package_name -%}
    {%- set package_name = none -%}
{%- endif -%}

{%- if package_name is none -%}
    {#- Same-project ref: no cross-project behavior to override -#}
    {{ return(builtins.ref(model_name, version=version)) }}
{%- endif -%}

{%- set rel = builtins.ref(package_name, model_name, version=version) -%}

{%- set upstream_env = var('xref_upstream_env', env_var('DBT_XREF_UPSTREAM_ENV', '')) -%}

{%- if upstream_env == '' -%}
    {#- Redirect not enabled for this run: behave exactly like ref() -#}
    {{ return(rel) }}
{%- endif -%}

{%- set env_map = var('xref_env_map', {}) -%}
{%- set env_cfg = env_map.get(upstream_env, {}) -%}
{%- set new_database = env_cfg.get('database', rel.database) -%}
{%- set new_schema = env_cfg.get('schema', rel.schema) -%}

{%- set exposed_models = var('xref_exposed_models', {}) -%}
{%- set exposed_key = package_name ~ '.' ~ model_name -%}
{%- set new_identifier = exposed_models.get(exposed_key, rel.identifier) -%}

{%- set new_rel = rel.replace_path(database=new_database, schema=new_schema, identifier=new_identifier) -%}

{%- if execute -%}
    {{ log("xref(): " ~ package_name ~ "." ~ model_name ~ " -> " ~ new_rel, info=true) }}
{%- endif -%}

{{ return(new_rel) }}

{% endmacro %}
