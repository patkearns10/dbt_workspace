# dbt lineage & compiled SQL

Two tiles over the dbt platform Discovery API, built to drop into BHP's
existing Streamlit in Snowflake app:

1. **Upstream DAG** for a selected model — the same set `+some_model` selects
2. **Compiled SQL** for that model, as dbt executed it in Snowflake

| File | Purpose |
| --- | --- |
| `dbt_lineage.py` | **The deliverable.** Importable module holding both tiles. This is what goes into the existing app. |
| `INTEGRATION.md` | How to add it to the existing app, and what it deliberately avoids doing to a host app. **Start here.** |
| `streamlit_app.py` | Local test harness — a minimal host app that does nothing but call the two tiles. Not deployed. |
| `check_connection.py` | Pre-flight check for the Discovery API. Run first. |
| `diagnose_lineage.py` | Deeper diagnostic for a rejected query (`400 Bad Request`). Bisects the lineage query and introspects the live schema. |
| `setup_external_access.sql` | One-time Snowflake setup to let the existing app reach the API (ACCOUNTADMIN). |
| `requirements-local.txt` | Local dependencies only. |

For integrating into the real app, read `INTEGRATION.md`. This page covers
local testing.

---

## Testing locally

`dbt_lineage.py` never assumes it's running inside Snowflake. It probes for a
Snowflake secret, falls back to environment variables, then to `st.secrets` — so
local runs need no code changes. The only feature that doesn't work locally is
the optional "Preview the built relation" panel, which needs a Snowpark session;
it degrades to an informational message.

### 1. What you need

- Python 3.9+
- A dbt **Metadata Only** service token — dbt platform → Account settings → Service tokens → New token
- Your **Discovery API URL** — Account settings → Access URLs. Multi-tenant format is
  `https://<ACCOUNT_PREFIX>.metadata.<REGION>.dbt.com/graphql`
  (`us1` North America AWS, `eu1` EMEA, `au1` APAC, `jp1` Japan)
- Your **environment ID** — the numeric ID in the environment's URL in dbt platform.
  Use the production environment; it's the one with successful runs and therefore metadata.

> If your dbt account has IP restrictions enabled, all service token traffic is
> subject to them. Your laptop's public IP needs to be on the allowlist, or you'll
> get a 403 locally even with a valid token.

### 2. Set up the environment

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements-local.txt
```

### 3. Export your config

```bash
export DBT_METADATA_URL="https://abc123.metadata.us1.dbt.com/graphql"
export DBT_SERVICE_TOKEN="dbtc_your_token_here"
export DBT_ENVIRONMENT_ID="12345"
```

All three scripts read these. In the deployed app none of them apply — the URL
and environment ID come from your `dbt_lineage.configure(...)` call and the token
from the bound Snowflake secret.

Prefer a file? Create `.streamlit/secrets.toml` instead of exporting the token:

```toml
dbt_metadata_token = "dbtc_your_token_here"
```

Either way, keep the token out of version control. Add `.streamlit/secrets.toml`
and `.env` to `.gitignore`.

### 4. Pre-flight check

Run this before touching the UI. It exercises the exact queries the tiles depend
on and gives you a specific error instead of a blank page:

```bash
python check_connection.py
```

```
dbt Discovery API pre-flight check
------------------------------------------------------------
  ok    config looks sane  (https://abc123.metadata.us1.dbt.com/graphql, environment 85030)
  ok    authenticated — 187 models in environment 85030
```

Then test lineage and SQL retrieval against a model you know:

```bash
python check_connection.py fct_orders
```

```
  ok    found model  model.bhp.fct_orders
  ok    lineage: +fct_orders resolves 9 upstream nodes (3 source, 6 model)
  ok    compiled SQL: 186 lines -> ANALYTICS.PROD.FCT_ORDERS (table, last run success)
```

### 5. Run the harness

```bash
streamlit run streamlit_app.py
```

Opens on `http://localhost:8501`. Pick a model; both tiles populate. This is a
throwaway host app — it exists so you can see the tiles working before wiring
them into the real one.

### 6. What to verify before deploying

Errors are easy to spot; a plausible-but-wrong graph is not. Worth doing once:

- Upstream node count for a known model matches `dbt ls -s +that_model` run
  against the project
- Compiled SQL matches `target/compiled/...` for the same model after a local
  `dbt compile`
- The model picker count matches the pre-flight count
- Depth slider at maximum shows the full closure; at 1, direct parents only
- Sources render as cylinders and terminate the graph — they have no upstream
- **Try a root model** (one built from literal `SELECT`s with no `ref()` or
  `source()`). It should report "no upstream dependencies", not error
- **Refresh** clears the module's cache only (lineage 15 min, model list 30 min)

---

## Troubleshooting

| Symptom | Cause |
| --- | --- |
| `400 Bad Request` | The request arrived and the **query** was rejected — not a network problem. The reason is in the response body, which is now surfaced in the error. Run `python diagnose_lineage.py <model>` to pinpoint it. |
| `401 Unauthorized` | Token wrong, expired, or lacks Discovery API access. Regenerate as Metadata Only. |
| `403 Forbidden` | dbt platform IP restrictions. Allowlist your IP locally, or Snowflake's egress CIDRs once deployed. |
| `404` | URL is missing the `/graphql` suffix, or the region/account prefix is wrong. |
| `Environment N returned null` | Wrong environment ID, or the token's project doesn't include it. |
| `0 models` | The environment has no successful job run yet — no metadata to query. |
| Empty `compiledCode` | Python model, or no successful run with compiled artifacts. |
| `has no upstream dependencies` | Not an error. The model is a DAG root — built from literal values with no `ref()`/`source()`. There is nothing for `+` to select. |
| Ancestors "could not be placed on the graph" | Cross-project refs. Listed below the DAG rather than dropped silently. |
| Network error once deployed | The app is missing its `EXTERNAL ACCESS INTEGRATION`. See `setup_external_access.sql`. |

Note: the Discovery API retains two months of history. Current-state lineage and
compiled code are always available; long-range trending is not.

### If you edit the GraphQL queries

Two non-obvious constraints, both of which cost real debugging time to find:

- **Keep the `executionInfo` aliases.** `executionInfo` returns a different type
  on each nested-node fragment (`ModelExecutionInfoNode`,
  `SnapshotExecutionInfoNode`, `SeedExecutionInfoNode!`), and `lastRunStatus` is
  `RunStatus` on some and `String` on others. GraphQL requires fields sharing a
  response key across sibling fragments to be mergeable, so selecting plain
  `executionInfo` on more than one fragment is rejected outright with a 400.
  Each fragment gets its own alias; `_fold_exec_aliases()` collapses them back.
- **Test the *combined* query, not each field alone.** Every field in the
  combined query validates fine in isolation. The conflict only appears when the
  fragments are selected together, which is why `diagnose_lineage.py` now has a
  scale/combined section as well as a per-field bisect.

---

## Deploying

See `INTEGRATION.md`. In short: add `dbt_lineage.py` to the app's stage, call
`configure()` then the two render functions, run `setup_external_access.sql`
(which ends in `ALTER STREAMLIT`, since the app already exists), and add
`requests` to the app's packages. No `graphviz` package is needed — the module
passes DOT text to `st.graphviz_chart`.
