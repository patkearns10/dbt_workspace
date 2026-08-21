# Adding this to an existing Streamlit in Snowflake app

`dbt_lineage.py` is written to drop into an app that already exists. This page
covers what changes on BHP's side and what to watch for.

---

## What the module deliberately does not do

These are the things that break — or quietly damage — a host app when code
written as a standalone script gets pasted into one. The module avoids all of
them, and it's worth knowing why:

| Avoided | Why it matters |
| --- | --- |
| `st.set_page_config()` | Only one call is allowed per app, and it must be the first Streamlit command. A second call raises `StreamlitAPIException`. Your app keeps ownership of the page config. |
| `st.stop()` | Halts the *entire* script run, so a dbt API hiccup would blank out your existing app. Errors surface in place and the functions return `None`. |
| `st.cache_data.clear()` | Clears every cache in the app, including your existing cached queries. `dbt_lineage.clear_cache()` clears only this module's three cached fetchers. |
| Sidebar writes | Nothing is added to your sidebar. All controls render inside the container you pass. |
| `st.title()` | Tiles use `st.subheader()`, so they read as sections of your page rather than claiming the page. |
| Bare widget keys | Every widget key is namespaced with `key_prefix` (default `dbt_lineage`), so nothing collides with your session state. |

---

## 1. Add the file to the app's stage

```sql
PUT file://dbt_lineage.py @<YOUR_DB>.<YOUR_SCHEMA>.<YOUR_APP_STAGE>
  AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
```

Put it alongside your existing main file. If you manage the app through
Snowsight's editor, add it as a new file in the same folder.

## 2. Configure once, near the top of your app

```python
import dbt_lineage

dbt_lineage.configure(
    metadata_url="https://<ACCOUNT_PREFIX>.metadata.<REGION>.dbt.com/graphql",
    environment_id=12345,
    # secret_name="dbt_metadata_token",  # only if you named the secret differently
)
```

The token is *not* passed here. It's read from the Snowflake secret bound to the
app. Regions are `us1` (North America AWS), `eu1` (EMEA), `au1` (APAC), `jp1`
(Japan). Both values come from dbt platform → Account settings → Access URLs and
the environment's URL.

## 3. Call the tiles wherever they belong

The functions render into whatever container you hand them, so they fit your
existing layout rather than dictating one.

```python
# Your app's own selector drives it
model = st.session_state["selected_model"]      # or however you already do this

tab_lineage, tab_sql = st.tabs(["Lineage", "SQL"])
dbt_lineage.render_dag_tile(model, container=tab_lineage)
dbt_lineage.render_sql_tile(model, container=tab_sql)
```

Other placements:

```python
# Side by side
left, right = st.columns(2)
dbt_lineage.render_dag_tile(model, container=left)
dbt_lineage.render_sql_tile(model, container=right)

# Collapsed, unobtrusive in a busy page
dbt_lineage.render_dag_tile(model, container=st.expander("Upstream lineage"))
dbt_lineage.render_sql_tile(model, container=st.expander("Compiled SQL"))

# Just the graph, no controls, fixed to the full closure
dbt_lineage.render_dag_tile(model, show_controls=False, show_table=False)

# Skip the Snowpark preview if your app already offers data preview
dbt_lineage.render_sql_tile(model, show_preview=False)
```

`model` takes either a bare name (`"fct_orders"`) or a unique_id
(`"model.bhp.fct_orders"`). Bare names are resolved against the model list and
raise a clear error if ambiguous.

If you'd rather not build a picker, `dbt_lineage.model_picker()` returns a
unique_id, and `dbt_lineage.list_models()` returns the full model list as a
DataFrame so you can build one in your app's own style.

### Return values

Both render functions return something useful, so the host app can react rather
than just draw. Both return `None` on failure, having already shown the error in
place.

`render_dag_tile()` returns a dict:

```python
result = dbt_lineage.render_dag_tile(model, container=tab)
if result and not result["is_root"]:
    st.write(f"{result['nodes_shown']} upstream nodes, {result['max_depth']} hops deep")
    result["table"]          # DataFrame of the upstream nodes
    result["counts"]         # {"Model": 6, "Source": 3, ...}
    result["unplaced"]       # ancestors that couldn't be graphed (cross-project refs)
```

`render_sql_tile()` returns the compiled SQL string, so you can feed it
somewhere else in your app — a diff view, a query editor, an export.

### Models with no upstream

A model built entirely from literal `SELECT`s — no `ref()`, no `source()` — is a
DAG root. There is nothing for `+` to select, so the tile says so plainly and
returns `is_root: True` rather than rendering an empty graph. This is a normal
thing to have in a project, not an error state.

Depth controls adapt to the shape of the lineage: no slider for a root model
(nothing to slide) or for a model whose entire closure is one hop away, and a
slider bounded by the real hop count otherwise.

### Rendering a tile twice on one page

Pass a distinct `key_prefix` each time, or the widgets will share state:

```python
dbt_lineage.render_dag_tile(model_a, key_prefix="dag_a", container=col_a)
dbt_lineage.render_dag_tile(model_b, key_prefix="dag_b", container=col_b)
```

## 4. Grant the existing app external access

This is the step most likely to stall the work, and it needs ACCOUNTADMIN.
Streamlit in Snowflake cannot make outbound HTTP calls without an external
access integration.

See `setup_external_access.sql`. Because the app already exists, the last step
is `ALTER STREAMLIT`, not `CREATE STREAMLIT`.

> **Watch this one.** `ALTER STREAMLIT ... SET EXTERNAL_ACCESS_INTEGRATIONS = (...)`
> *replaces* the list rather than appending to it. If the app already uses other
> integrations, run `DESCRIBE STREAMLIT <app>` first and include the existing
> ones in your `SET`. Same for `SECRETS`. Dropping an integration your app
> already depends on is an easy and confusing way to break it.

## 5. Add the package

The module needs `requests`. `pandas` and `streamlit` are almost certainly
already there.

- Snowsight editor: add `requests` via the **Packages** menu
- Stage-managed app: add `requests` to the app's `environment.yml`

No `graphviz` package is needed — the module passes DOT text to
`st.graphviz_chart`.

---

## Testing before you touch the real app

Test the module standalone first. `streamlit_app.py` in this folder is a minimal
host app that does nothing but call the two tiles, so you can confirm the
queries and rendering work before integrating. See the local testing walkthrough
in `README.md` for the full detail.

Locally there is no Snowflake secret to read from, so the module falls back to
environment variables. Set all three before running anything:

```bash
export DBT_METADATA_URL="https://<ACCOUNT_PREFIX>.metadata.<REGION>.dbt.com/graphql"
export DBT_SERVICE_TOKEN="dbtc_your_metadata_only_token"
export DBT_ENVIRONMENT_ID="12345"
```

Where each comes from:

| Variable | Source |
| --- | --- |
| `DBT_METADATA_URL` | dbt platform → Account settings → **Access URLs**. Regions: `us1` North America AWS, `eu1` EMEA, `au1` APAC, `jp1` Japan. Must end in `/graphql`. |
| `DBT_SERVICE_TOKEN` | dbt platform → Account settings → Service tokens → New token. **Metadata Only** is enough. |
| `DBT_ENVIRONMENT_ID` | The numeric ID in the environment's URL. Use production — it's the one with successful runs, and therefore metadata. |

Prefer a file over exports? Put the token in `.streamlit/secrets.toml` instead:

```toml
dbt_metadata_token = "dbtc_your_metadata_only_token"
```

Either way, keep the token out of version control — add `.streamlit/secrets.toml`
and `.env` to `.gitignore`. If your dbt account has IP restrictions enabled, all
service token traffic is subject to them, so your laptop's public IP needs to be
on the allowlist or you'll get a 403 locally even with a valid token.

Setup, once per machine:

```bash
python3 -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements-local.txt
```

Then, in the order that saves the most time:

1. `python check_connection.py` — validates URL, token, and environment ID with
   no Streamlit involved
2. `python check_connection.py fct_orders` — exercises the lineage and SQL queries
3. `python diagnose_lineage.py fct_orders` — only if something 400s. Bisects the
   lineage query field by field, reproduces it at real scale, and introspects
   your live schema to show what the endpoint actually accepts
4. `streamlit run streamlit_app.py` — confirms rendering
5. Then integrate, and verify in a dev copy of the app before production

Try a **root model** in step 4 as well as a normal one — a model built from
literal `SELECT`s with no `ref()` or `source()`. It should report "no upstream
dependencies" rather than erroring.

None of these variables matter once deployed. In Snowflake the token comes from
the bound secret, and the URL and environment ID come from your
`dbt_lineage.configure(...)` call.

## Verifying correctness, not just absence of errors

The tiles will happily render something plausible but wrong if a query is
subtly off. Two checks worth doing once:

- Upstream node count for a known model should match `dbt ls -s +that_model`
  run against the project
- The compiled SQL should match `target/compiled/...` for the same model after
  a local `dbt compile`

---

## Troubleshooting in a host app

| Symptom | Cause |
| --- | --- |
| `StreamlitAPIException: set_page_config() can only be called once` | Your app called it after something else rendered, or `streamlit_app.py` from this folder got deployed as a second entry point. The module never calls it. |
| Widgets resetting or fighting each other | Two tiles sharing a `key_prefix`. Give each a distinct one. |
| Your app's own cached queries keep re-running | Something is calling `st.cache_data.clear()`. Use `dbt_lineage.clear_cache()`. |
| `dbt lineage not configured` warning | `configure()` wasn't called, or the secret isn't bound. The tile warns and returns rather than breaking the page. |
| Network error only after deploy | The `ALTER STREAMLIT` step is missing, or a `SET` replaced the integration list. |
| `403 Forbidden` | dbt platform IP restrictions. Allowlist Snowflake's egress CIDRs. |
| `400 Bad Request` | The query was rejected — the request did arrive, so this is not a networking or auth problem. The response body is surfaced in the error. Run `diagnose_lineage.py`. |
| `has no upstream dependencies` | Not an error. The model is a DAG root, built from literal values with no `ref()`/`source()`. |

### If you modify the GraphQL queries

Two non-obvious constraints, both of which cost real debugging time:

- **Keep the `executionInfo` aliases.** That field returns a different type on
  each nested-node fragment (`ModelExecutionInfoNode`,
  `SnapshotExecutionInfoNode`, `SeedExecutionInfoNode!`), and `lastRunStatus` is
  `RunStatus` on some and `String` on others. GraphQL requires fields sharing a
  response key across sibling fragments to be mergeable, so selecting plain
  `executionInfo` on more than one fragment is rejected with a 400. Each
  fragment carries its own alias; `_fold_exec_aliases()` collapses them back to
  a single `executionInfo` key for everything downstream.
- **Validate the combined query, not each field alone.** Every field validates
  fine in isolation — the conflict only appears when the fragments are selected
  together. `diagnose_lineage.py` tests both, for exactly this reason.

Also worth knowing: `parents` is one of the most complex fields in this API, so
parent-edge reconstruction batches at `EDGE_CHUNK = 50` and halves recursively
on rejection. Don't raise it to 500 without testing on your largest model.

The Discovery API retains two months of history. Current-state lineage and
compiled code are always available; long-range trending is not.
