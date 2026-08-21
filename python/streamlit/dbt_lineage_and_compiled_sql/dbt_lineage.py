"""
dbt lineage & compiled SQL — drop-in module for an existing Streamlit app
=========================================================================

Designed to be added to an app that already exists. It deliberately does not:

  * call st.set_page_config()      — the host app owns that, and a second call raises
  * call st.stop()                 — that would halt the host app; errors return instead
  * call st.cache_data.clear()     — that would nuke the host app's caches too;
                                     only this module's caches are cleared
  * write to the sidebar           — nothing is added to the host's sidebar
  * claim a page title             — tiles render as sections, in your container

Every widget key is namespaced via `key_prefix`, so nothing collides with the
host app's session state.


PUBLIC API
----------
    configure(metadata_url=..., environment_id=..., token=...)
        Set module defaults once, e.g. at the top of your app.

    list_models(cfg=None) -> pd.DataFrame
        All models in the environment. Useful for building your own picker.

    model_picker(cfg=None, container=None, key_prefix=...) -> str | None
        Optional ready-made selectbox. Returns a unique_id.

    render_dag_tile(model, cfg=None, container=None, key_prefix=...) -> dict | None
        Tile 1 — upstream DAG (the set `+some_model` selects).

    render_sql_tile(model, cfg=None, container=None, key_prefix=...) -> str | None
        Tile 2 — compiled SQL.

    clear_cache()
        Clears only this module's cached queries.

`model` accepts either a unique_id ("model.bhp.fct_orders") or a bare model
name ("fct_orders"), which is resolved for you.


MINIMAL INTEGRATION
-------------------
    import dbt_lineage

    dbt_lineage.configure(
        metadata_url="https://abc123.metadata.us1.dbt.com/graphql",
        environment_id=12345,
    )

    model = dbt_lineage.model_picker()
    if model:
        lineage_tab, sql_tab = st.tabs(["Lineage", "SQL"])
        dbt_lineage.render_dag_tile(model, container=lineage_tab)
        dbt_lineage.render_sql_tile(model, container=sql_tab)

Driving it from your app's own selector instead:

    dbt_lineage.render_dag_tile(my_selected_model_name, container=st.expander("Lineage"))
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd
import requests
import streamlit as st

__all__ = [
    "DbtConfig",
    "ConfigError",
    "DiscoveryError",
    "configure",
    "get_config",
    "list_models",
    "model_picker",
    "render_dag_tile",
    "render_sql_tile",
    "clear_cache",
]

PAGE_SIZE = 500  # max allowed by the Discovery API, used for the model list

# Parent-edge reconstruction asks for `parents` on many nodes at once. The docs
# call nested nodes like `parents` among the most complex fields and advise
# breaking such queries up — asking for 500 at once trips a complexity/response
# size limit and returns HTTP 400. Chunk conservatively and back off further if
# the API still objects.
EDGE_CHUNK = 50
EDGE_CHUNK_FLOOR = 5

DEFAULT_SECRET_NAME = "dbt_metadata_token"
DEFAULT_KEY_PREFIX = "dbt_lineage"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class ConfigError(RuntimeError):
    pass


class DiscoveryError(RuntimeError):
    pass


@dataclass(frozen=True)
class DbtConfig:
    metadata_url: str
    environment_id: int
    token: str


# Module defaults, set by configure(). Falls back to environment variables so
# the same file works when tested locally.
_defaults: Dict[str, Any] = {
    "metadata_url": os.environ.get("DBT_METADATA_URL"),
    "environment_id": os.environ.get("DBT_ENVIRONMENT_ID"),
    "token": None,
    "secret_name": DEFAULT_SECRET_NAME,
}


def configure(
    metadata_url: Optional[str] = None,
    environment_id: Optional[int] = None,
    token: Optional[str] = None,
    secret_name: Optional[str] = None,
) -> None:
    """Set module-level defaults. Call once near the top of the host app.

    metadata_url    Account settings -> Access URLs, e.g.
                    https://<ACCOUNT_PREFIX>.metadata.<REGION>.dbt.com/graphql
    environment_id  Numeric ID from the environment's URL in dbt platform.
    token           Usually omit — resolved from the Snowflake SECRET.
    secret_name     Name bound in the app's SECRETS clause. Default
                    'dbt_metadata_token'.
    """
    if metadata_url is not None:
        _defaults["metadata_url"] = metadata_url
    if environment_id is not None:
        _defaults["environment_id"] = environment_id
    if token is not None:
        _defaults["token"] = token
    if secret_name is not None:
        _defaults["secret_name"] = secret_name


def _resolve_token(secret_name: str) -> Optional[str]:
    """Snowflake SECRET -> env var -> st.secrets."""
    try:
        import _snowflake  # only present inside Streamlit in Snowflake

        return _snowflake.get_generic_secret_string(secret_name)
    except Exception:
        pass

    if os.environ.get("DBT_SERVICE_TOKEN"):
        return os.environ["DBT_SERVICE_TOKEN"]

    try:
        return st.secrets[secret_name]
    except Exception:
        return None


def get_config(
    metadata_url: Optional[str] = None,
    environment_id: Optional[int] = None,
    token: Optional[str] = None,
    secret_name: Optional[str] = None,
) -> DbtConfig:
    """Build a config from explicit args, then module defaults, then env vars."""
    url = metadata_url or _defaults.get("metadata_url")
    env_id = environment_id or _defaults.get("environment_id")
    secret = secret_name or _defaults.get("secret_name") or DEFAULT_SECRET_NAME
    tok = token or _defaults.get("token") or _resolve_token(secret)

    if not url:
        raise ConfigError(
            "No Discovery API URL. Call dbt_lineage.configure(metadata_url=...) "
            "with the value from dbt platform: Account settings -> Access URLs."
        )
    if not str(url).rstrip("/").endswith("/graphql"):
        raise ConfigError(f"Discovery API URL must end in /graphql — got {url}")
    if not env_id:
        raise ConfigError(
            "No environment ID. Call dbt_lineage.configure(environment_id=...) "
            "with the numeric ID from the environment's URL in dbt platform."
        )
    if not tok:
        raise ConfigError(
            f"No dbt service token found. Expected a Snowflake SECRET named "
            f"'{secret}' bound to this Streamlit app via its SECRETS clause. "
            "See setup_external_access.sql."
        )

    return DbtConfig(str(url), int(env_id), str(tok))


# ---------------------------------------------------------------------------
# Discovery API client
# ---------------------------------------------------------------------------


def _run_query(
    url: str, token: str, query: str, variables: Dict[str, Any]
) -> Dict[str, Any]:
    resp = requests.post(
        url,
        headers={"authorization": f"Bearer {token}", "content-type": "application/json"},
        json={"query": query, "variables": variables},
        timeout=60,
    )
    if resp.status_code == 401:
        raise DiscoveryError(
            "401 Unauthorized — the dbt service token is wrong, expired, or lacks "
            "Discovery API access. A 'Metadata Only' token is sufficient."
        )
    if resp.status_code == 403:
        raise DiscoveryError(
            "403 Forbidden — dbt platform IP restrictions are blocking this request. "
            "Add Snowflake's egress CIDRs to the dbt allowlist."
        )
    if resp.status_code == 404:
        raise DiscoveryError(f"404 from {url} — check the URL and region.")

    if resp.status_code >= 400:
        # Do NOT use raise_for_status() here. It raises requests.HTTPError, a
        # subclass of RequestException, which makes a server-side rejection look
        # like a network failure. For GraphQL a 400 means the request arrived
        # fine and the *query* was rejected — the reason is in the body, so
        # surface it rather than hiding it.
        detail = ""
        try:
            body = resp.json()
            if body.get("errors"):
                detail = "\n".join(
                    f"  • {e.get('message', e)}" for e in body["errors"]
                )
            else:
                detail = json.dumps(body, indent=2)[:2000]
        except ValueError:
            detail = resp.text[:2000]

        raise DiscoveryError(
            f"HTTP {resp.status_code} from the Discovery API. The request "
            f"reached the server, so networking and auth are fine — the query "
            f"itself was rejected:\n\n{detail or '(empty response body)'}"
        )

    payload = resp.json()
    if payload.get("errors"):
        detail = "\n".join(f"  • {e.get('message', e)}" for e in payload["errors"])
        raise DiscoveryError(f"GraphQL errors:\n\n{detail}")
    return payload["data"]


_MODEL_LIST_QUERY = """
query ModelList($environmentId: BigInt!, $first: Int!, $after: String) {
  environment(id: $environmentId) {
    applied {
      models(first: $first, after: $after) {
        totalCount
        pageInfo { endCursor hasNextPage }
        edges {
          node { uniqueId name database schema alias materializedType }
        }
      }
    }
  }
}
"""

# `ancestors` returns the full upstream closure as a flat list with no edges,
# so we fetch the closure here and reconstruct edges with _PARENT_EDGES_QUERY.
# NOTE ON THE ALIASES BELOW — do not remove them.
#
# `executionInfo` returns a DIFFERENT type on each nested-node fragment
# (ModelExecutionInfoNode, SnapshotExecutionInfoNode, SeedExecutionInfoNode!),
# and `lastRunStatus` is `RunStatus` on some and `String` on others. GraphQL
# requires fields sharing a response key across sibling fragments to be
# mergeable, so selecting plain `executionInfo` on more than one fragment is a
# hard validation error and the whole query is rejected with HTTP 400:
#
#   Fields "executionInfo" conflict because they return conflicting types
#   "ModelExecutionInfoNode" and "SeedExecutionInfoNode!".
#
# Giving each fragment its own alias makes the response keys distinct, so no
# merge is attempted. _fetch_upstream folds them back to `executionInfo`.
_EXEC_ALIASES = ("modelExecutionInfo", "snapshotExecutionInfo", "seedExecutionInfo")

_ANCESTORS_QUERY = """
query Ancestors($environmentId: BigInt!, $uniqueId: String!) {
  environment(id: $environmentId) {
    applied {
      models(first: 1, filter: { uniqueIds: [$uniqueId] }) {
        edges {
          node {
            uniqueId
            name
            # The target's OWN metadata. `ancestors` covers upstream nodes only,
            # so without these the selected model is the one row in the table
            # with no materialization, status, or build time. Safe to select
            # unaliased: this is a different selection set from the fragments
            # below, so no field merging applies.
            materializedType
            executionInfo { lastRunStatus executeCompletedAt }
            ancestors(types: [Model, Source, Seed, Snapshot]) {
              ... on ModelAppliedStateNestedNode {
                uniqueId name resourceType materializedType
                modelExecutionInfo: executionInfo {
                  lastRunStatus executeCompletedAt
                }
              }
              ... on SourceAppliedStateNestedNode {
                uniqueId name sourceName resourceType
                freshness { maxLoadedAt freshnessStatus }
              }
              ... on SnapshotAppliedStateNestedNode {
                uniqueId name resourceType
                snapshotExecutionInfo: executionInfo {
                  lastRunStatus executeCompletedAt
                }
              }
              ... on SeedAppliedStateNestedNode {
                uniqueId name resourceType
                seedExecutionInfo: executionInfo {
                  lastRunStatus executeCompletedAt
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

# Reduced variant. Some Discovery API deployments reject optional subfields on
# the nested-node fragments (freshnessStatus and materializedType are the usual
# culprits). If the full query 400s, we retry with only the fields the graph
# strictly needs, so the tile degrades instead of failing outright.
_ANCESTORS_QUERY_MINIMAL = """
query AncestorsMinimal($environmentId: BigInt!, $uniqueId: String!) {
  environment(id: $environmentId) {
    applied {
      models(first: 1, filter: { uniqueIds: [$uniqueId] }) {
        edges {
          node {
            uniqueId
            name
            materializedType
            ancestors(types: [Model, Source, Seed, Snapshot]) {
              ... on ModelAppliedStateNestedNode { uniqueId name }
              ... on SourceAppliedStateNestedNode { uniqueId name sourceName }
              ... on SnapshotAppliedStateNestedNode { uniqueId name }
              ... on SeedAppliedStateNestedNode { uniqueId name }
            }
          }
        }
      }
    }
  }
}
"""

# Split by resource type rather than one combined query, so a filter that this
# deployment doesn't support on snapshots cannot take the model edges down with
# it. Snapshot edges are best-effort.
_MODEL_PARENTS_QUERY = """
query ModelParents($environmentId: BigInt!, $uniqueIds: [String!], $first: Int!) {
  environment(id: $environmentId) {
    applied {
      models(first: $first, filter: { uniqueIds: $uniqueIds }) {
        edges { node { uniqueId parents { uniqueId name resourceType } } }
      }
    }
  }
}
"""

_SNAPSHOT_PARENTS_QUERY = """
query SnapshotParents($environmentId: BigInt!, $uniqueIds: [String!], $first: Int!) {
  environment(id: $environmentId) {
    applied {
      snapshots(first: $first, filter: { uniqueIds: $uniqueIds }) {
        edges { node { uniqueId parents { uniqueId name resourceType } } }
      }
    }
  }
}
"""

_COMPILED_SQL_QUERY = """
query CompiledSql($environmentId: BigInt!, $uniqueId: String!) {
  environment(id: $environmentId) {
    applied {
      models(first: 1, filter: { uniqueIds: [$uniqueId] }) {
        edges {
          node {
            uniqueId name database schema alias materializedType compiledCode
            executionInfo {
              lastRunStatus lastRunError executeCompletedAt
              executionTime lastRunId lastJobDefinitionId
            }
          }
        }
      }
    }
  }
}
"""


# --- Cached fetchers -------------------------------------------------------
# Cache keys include url/env/token so multiple environments can coexist.
# These are cleared individually by clear_cache(), never via
# st.cache_data.clear(), which would also wipe the host app's caches.


@st.cache_data(ttl=1800, show_spinner=False)
def _fetch_models(url: str, token: str, env_id: int) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    cursor = None
    while True:
        data = _run_query(
            url,
            token,
            _MODEL_LIST_QUERY,
            {"environmentId": env_id, "first": PAGE_SIZE, "after": cursor},
        )
        env = data.get("environment")
        if not env:
            raise DiscoveryError(
                f"Environment {env_id} returned null. Either the ID is wrong, or "
                "this token has no access to that environment's project."
            )
        conn = env["applied"]["models"]
        rows.extend(edge["node"] for edge in conn["edges"])
        page = conn["pageInfo"]
        if not page["hasNextPage"]:
            break
        cursor = page["endCursor"]

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("name").reset_index(drop=True)
    return df


def _fold_exec_aliases(node: Dict[str, Any]) -> Dict[str, Any]:
    """Collapse the per-fragment executionInfo aliases back to `executionInfo`.

    The aliases exist only to satisfy GraphQL field-merging rules (see the note
    above _ANCESTORS_QUERY). Everything downstream expects one key.
    """
    out = dict(node)
    found = None
    for alias in _EXEC_ALIASES:
        value = out.pop(alias, None)
        if value and found is None:
            found = value
    if found is not None:
        out["executionInfo"] = found
    return out


@st.cache_data(ttl=900, show_spinner=False)
def _fetch_upstream(
    url: str, token: str, env_id: int, unique_id: str
) -> Tuple[Dict[str, Dict[str, Any]], List[Tuple[str, str]]]:
    """Return (nodes_by_unique_id, edges) for the upstream closure of unique_id."""
    variables = {"environmentId": env_id, "uniqueId": unique_id}
    try:
        data = _run_query(url, token, _ANCESTORS_QUERY, variables)
    except DiscoveryError as full_exc:
        # Retry without the optional metadata subfields before giving up, so an
        # unsupported field costs us run status rather than the whole graph.
        try:
            data = _run_query(url, token, _ANCESTORS_QUERY_MINIMAL, variables)
        except DiscoveryError:
            raise full_exc from None

    edges_in = data["environment"]["applied"]["models"]["edges"]
    if not edges_in:
        raise DiscoveryError(f"Model not found in this environment: {unique_id}")

    target = edges_in[0]["node"]
    nodes: Dict[str, Dict[str, Any]] = {
        target["uniqueId"]: {
            "uniqueId": target["uniqueId"],
            "name": target["name"],
            "resourceType": "Model",
            # Carry the target's own metadata through, so the selected model is
            # not the one blank row in the table. Absent when the minimal
            # fallback query was used.
            "materializedType": target.get("materializedType"),
            "executionInfo": target.get("executionInfo"),
            "is_target": True,
        }
    }
    for anc in target.get("ancestors") or []:
        if anc and anc.get("uniqueId"):
            nodes[anc["uniqueId"]] = {
                **_fold_exec_aliases(anc),
                "is_target": False,
            }

    # Only models and snapshots have upstream deps; sources and seeds are roots.
    resolvable = [
        uid for uid in nodes if uid.startswith("model.") or uid.startswith("snapshot.")
    ]
    edges: Set[Tuple[str, str]] = set()

    def absorb(data: Dict[str, Any], key: str) -> None:
        for edge in data["environment"]["applied"].get(key, {}).get("edges", []):
            child = edge["node"]["uniqueId"]
            for parent in edge["node"].get("parents") or []:
                pid = parent.get("uniqueId")
                # Keep only in-closure parents, so the graph stays scoped to
                # +some_model.
                if pid and pid in nodes:
                    edges.add((pid, child))

    def collect(ids: List[str], query: str, key: str, required: bool) -> None:
        """Fetch parent edges for `ids`, halving the batch on rejection.

        A 400 here is usually query complexity rather than bad syntax, so a
        smaller batch of the same query succeeds. Recursing down to single IDs
        means one pathological node cannot cost us the whole graph.
        """
        if not ids:
            return
        try:
            absorb(
                _run_query(
                    url,
                    token,
                    query,
                    {
                        "environmentId": env_id,
                        "uniqueIds": ids,
                        "first": max(len(ids), 1),
                    },
                ),
                key,
            )
        except DiscoveryError:
            if len(ids) > EDGE_CHUNK_FLOOR:
                mid = len(ids) // 2
                collect(ids[:mid], query, key, required)
                collect(ids[mid:], query, key, required)
            elif required:
                raise
            # else: best-effort, drop these edges silently

    models = [uid for uid in resolvable if uid.startswith("model.")]
    snapshots = [uid for uid in resolvable if uid.startswith("snapshot.")]

    for start in range(0, len(models), EDGE_CHUNK):
        collect(
            models[start : start + EDGE_CHUNK],
            _MODEL_PARENTS_QUERY,
            "models",
            required=True,
        )
    for start in range(0, len(snapshots), EDGE_CHUNK):
        collect(
            snapshots[start : start + EDGE_CHUNK],
            _SNAPSHOT_PARENTS_QUERY,
            "snapshots",
            required=False,
        )

    return nodes, sorted(edges)


@st.cache_data(ttl=900, show_spinner=False)
def _fetch_compiled_sql(
    url: str, token: str, env_id: int, unique_id: str
) -> Dict[str, Any]:
    data = _run_query(
        url, token, _COMPILED_SQL_QUERY, {"environmentId": env_id, "uniqueId": unique_id}
    )
    edges = data["environment"]["applied"]["models"]["edges"]
    if not edges:
        raise DiscoveryError(f"Model not found in this environment: {unique_id}")
    return edges[0]["node"]


def clear_cache() -> None:
    """Clear only this module's caches. Never calls st.cache_data.clear(),
    which would also discard the host app's cached data."""
    _fetch_models.clear()
    _fetch_upstream.clear()
    _fetch_compiled_sql.clear()


# ---------------------------------------------------------------------------
# Graph helpers (pure — safe to unit test)
# ---------------------------------------------------------------------------

NODE_STYLE = {
    # resourceType -> (fillcolor, shape)
    "Model": ("#E6F0FF", "box"),
    "Source": ("#E9F7EF", "cylinder"),
    "Seed": ("#FDF3E3", "note"),
    "Snapshot": ("#F3E9F7", "component"),
}


def normalize_type(node: Dict[str, Any]) -> str:
    uid = node["uniqueId"]
    for prefix, label in (
        ("model.", "Model"),
        ("source.", "Source"),
        ("seed.", "Seed"),
        ("snapshot.", "Snapshot"),
    ):
        if uid.startswith(prefix):
            return label
    return "Model"


def compute_depths(target: str, edges: Sequence[Tuple[str, str]]) -> Dict[str, int]:
    """Breadth-first hop count upstream from target. Target is depth 0."""
    parents_of: Dict[str, List[str]] = {}
    for parent, child in edges:
        parents_of.setdefault(child, []).append(parent)

    depths = {target: 0}
    frontier = [target]
    while frontier:
        nxt = []
        for node in frontier:
            for parent in parents_of.get(node, []):
                if parent not in depths:
                    depths[parent] = depths[node] + 1
                    nxt.append(parent)
        frontier = nxt
    return depths


def build_dot(
    nodes: Dict[str, Dict[str, Any]],
    edges: Sequence[Tuple[str, str]],
    keep: Set[str],
    rankdir: str = "LR",
) -> str:
    """Render the graph as Graphviz DOT text. Passing DOT to st.graphviz_chart
    avoids needing the `graphviz` package in the Snowflake environment."""
    lines = [
        "digraph lineage {",
        f'  rankdir="{rankdir}";',
        '  graph [bgcolor="transparent", pad="0.3", nodesep="0.25", ranksep="0.7"];',
        '  node [style="filled,rounded", fontname="Helvetica", fontsize="10", '
        'color="#9AA5B1", penwidth="1"];',
        '  edge [color="#9AA5B1", arrowsize="0.7"];',
    ]

    for uid in keep:
        node = nodes[uid]
        rtype = normalize_type(node)
        fill, shape = NODE_STYLE[rtype]
        label = node.get("name") or uid
        if rtype == "Source" and node.get("sourceName"):
            label = f"{node['sourceName']}.{label}"

        attrs = [f'label="{label}"', f'fillcolor="{fill}"', f'shape="{shape}"']
        if node.get("is_target"):
            attrs += ['penwidth="2.5"', 'color="#FF694A"', 'fillcolor="#FFE8E2"']

        status = (node.get("executionInfo") or {}).get("lastRunStatus")
        if status in ("error", "fail"):
            attrs += ['color="#C0392B"', 'penwidth="2"']

        lines.append(f'  "{uid}" [{", ".join(attrs)}];')

    for parent, child in edges:
        if parent in keep and child in keep:
            lines.append(f'  "{parent}" -> "{child}";')

    lines.append("}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _container(container: Any) -> Any:
    """Return a context manager to render into. Defaults to a fresh container
    in the main body, so the module never assumes anything about the layout."""
    return container if container is not None else st.container()


def _resolve_model(cfg: DbtConfig, model: str) -> str:
    """Accept either a unique_id or a bare model name; return a unique_id."""
    if model.startswith(("model.", "snapshot.", "seed.", "source.")):
        return model

    df = _fetch_models(cfg.metadata_url, cfg.token, cfg.environment_id)
    matches = df[df["name"] == model] if not df.empty else df
    if matches.empty:
        raise DiscoveryError(
            f"No model named '{model}' in environment {cfg.environment_id}."
        )
    if len(matches) > 1:
        raise DiscoveryError(
            f"'{model}' is ambiguous ({len(matches)} matches). Pass a unique_id: "
            + ", ".join(matches["uniqueId"].tolist()[:5])
        )
    return matches.iloc[0]["uniqueId"]


def _handle(exc: Exception, what: str) -> None:
    """Surface an error in place. Never st.stop() — that would halt the host app."""
    if isinstance(exc, ConfigError):
        st.warning(f"dbt lineage not configured: {exc}")
    elif isinstance(exc, DiscoveryError):
        st.error(f"{what} failed: {exc}")
    elif isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        # Only a genuine transport failure gets the network advice. A 4xx/5xx
        # is raised as DiscoveryError above, because the request did arrive.
        st.error(
            f"{what} failed — could not reach the Discovery API: {exc}\n\n"
            "Confirm this Streamlit app has an EXTERNAL ACCESS INTEGRATION "
            "covering the Discovery API host."
        )
    elif isinstance(exc, requests.RequestException):
        st.error(f"{what} failed: {exc}")
    else:
        st.error(f"{what} failed: {exc}")


# ---------------------------------------------------------------------------
# Public: model list and optional picker
# ---------------------------------------------------------------------------


def list_models(cfg: Optional[DbtConfig] = None, **cfg_kwargs: Any) -> pd.DataFrame:
    """Every model in the environment, as a DataFrame. Raises on failure —
    use this to build your own picker with your app's own styling."""
    cfg = cfg or get_config(**cfg_kwargs)
    return _fetch_models(cfg.metadata_url, cfg.token, cfg.environment_id)


def model_picker(
    cfg: Optional[DbtConfig] = None,
    container: Any = None,
    key_prefix: str = DEFAULT_KEY_PREFIX,
    label: str = "dbt model",
    show_refresh: bool = True,
    **cfg_kwargs: Any,
) -> Optional[str]:
    """Optional ready-made selectbox. Returns a unique_id, or None on failure.

    Skip this if the host app already has a selector — pass its value straight
    to render_dag_tile / render_sql_tile instead.
    """
    with _container(container):
        try:
            cfg = cfg or get_config(**cfg_kwargs)
            df = _fetch_models(cfg.metadata_url, cfg.token, cfg.environment_id)
        except Exception as exc:  # noqa: BLE001
            _handle(exc, "Loading models")
            return None

        if df.empty:
            st.warning(
                f"No models in environment {cfg.environment_id}. The environment "
                "needs at least one successful job run before metadata appears."
            )
            return None

        labels = {
            f"{row['name']}  ·  {row['schema']}": row["uniqueId"]
            for _, row in df.iterrows()
        }

        if show_refresh:
            pick_col, btn_col = st.columns([5, 1])
        else:
            pick_col, btn_col = st.container(), None

        with pick_col:
            choice = st.selectbox(
                f"{label}  ({len(df):,} in environment {cfg.environment_id})",
                options=sorted(labels),
                key=f"{key_prefix}_model",
            )
        if btn_col is not None:
            with btn_col:
                st.write("")
                if st.button(
                    "Refresh", key=f"{key_prefix}_refresh", use_container_width=True
                ):
                    clear_cache()
                    st.rerun()

        return labels[choice]


# ---------------------------------------------------------------------------
# Tile 1 — upstream DAG
# ---------------------------------------------------------------------------


def render_dag_tile(
    model: str,
    cfg: Optional[DbtConfig] = None,
    container: Any = None,
    key_prefix: str = DEFAULT_KEY_PREFIX,
    heading: Optional[str] = None,
    show_controls: bool = True,
    show_table: bool = True,
    default_depth: Optional[int] = None,
    rankdir: str = "LR",
    **cfg_kwargs: Any,
) -> Optional[Dict[str, Any]]:
    """Upstream DAG for `model` — the set that `+some_model` selects.

    model          unique_id or bare model name.
    container      Where to render (tab, column, expander). Defaults to a
                   fresh container in the main body.
    key_prefix     Namespace for widget keys. Change it if you render this
                   tile more than once on the same page.
    show_controls  Depth slider, layout toggle, type filter.
    default_depth  Initial depth. None = full closure (true +some_model).
    rankdir        "LR" or "TB" when show_controls is False.

    Returns a summary dict, or None on failure.
    """
    with _container(container):
        try:
            cfg = cfg or get_config(**cfg_kwargs)
            unique_id = _resolve_model(cfg, model)
            nodes, edges = _fetch_upstream(
                cfg.metadata_url, cfg.token, cfg.environment_id, unique_id
            )
        except Exception as exc:  # noqa: BLE001
            _handle(exc, "Lineage query")
            return None

        name = nodes[unique_id].get("name", unique_id)
        st.subheader(heading if heading is not None else f"Upstream DAG  ·  `+{name}`")

        depths = compute_depths(unique_id, edges)
        max_depth = max(depths.values()) if depths else 0
        depth = default_depth or max(max_depth, 1)
        types = ["Model", "Source", "Seed", "Snapshot"]

        # A model with no upstream at all (max_depth 0) or exactly one hop
        # (max_depth 1) leaves nothing to slide, and st.slider raises if
        # min_value == max_value. Root models are legitimate — a seed-style
        # model built from literal SELECTs has no refs or sources — so render
        # the tile without depth controls rather than erroring.
        is_root = max_depth == 0
        show_depth_slider = show_controls and max_depth >= 2

        if is_root:
            st.info(
                f"`{name}` has no upstream dependencies — it is a root of the "
                "DAG. Nothing for `+` to select."
            )
            st.caption(
                "Expected when a model is built entirely from literal values or "
                "hardcoded SELECTs, with no `ref()` or `source()` calls."
            )
            return {
                "unique_id": unique_id,
                "name": name,
                "nodes_shown": 1,
                "max_depth": 0,
                "counts": {},
                "unplaced": sorted(set(nodes) - set(depths)),
                "table": pd.DataFrame(),
                "is_root": True,
            }

        if show_controls:
            c1, c2, c3 = st.columns([2, 2, 3])
            with c1:
                if show_depth_slider:
                    depth = st.slider(
                        "Upstream depth (hops)",
                        min_value=1,
                        max_value=max_depth,
                        value=min(depth, max_depth),
                        key=f"{key_prefix}_depth",
                        help="1 = direct parents only. The maximum is the full "
                        "closure, which is what `+some_model` selects.",
                    )
                else:
                    # max_depth == 1: direct parents are the entire closure.
                    depth = 1
                    st.metric("Upstream depth", "1 hop")
                    st.caption("Direct parents only — that is the full closure.")
            with c2:
                rankdir = (
                    "LR"
                    if st.radio(
                        "Layout",
                        ["Left → right", "Top → bottom"],
                        key=f"{key_prefix}_layout",
                    )
                    == "Left → right"
                    else "TB"
                )
            with c3:
                types = st.multiselect(
                    "Node types", types, default=types, key=f"{key_prefix}_types"
                )

        keep = {
            uid
            for uid, d in depths.items()
            if d <= depth and (uid == unique_id or normalize_type(nodes[uid]) in types)
        }
        # Ancestors the edge reconstruction could not place (rare — cross-project
        # refs) are reported rather than silently dropped.
        unplaced = sorted(set(nodes) - set(depths))

        counts = pd.Series(
            [normalize_type(nodes[u]) for u in keep if u != unique_id]
        ).value_counts()
        cols = st.columns(5)
        cols[0].metric("Nodes shown", len(keep))
        for i, label in enumerate(["Model", "Source", "Snapshot", "Seed"]):
            cols[i + 1].metric(label + "s", int(counts.get(label, 0)))

        if len(keep) <= 1:
            st.info("This model has no upstream dependencies at the selected depth.")
        else:
            st.graphviz_chart(
                build_dot(nodes, edges, keep, rankdir), use_container_width=True
            )

        if unplaced:
            st.caption(
                f"{len(unplaced)} ancestor(s) could not be placed on the graph "
                "(typically cross-project refs): " + ", ".join(unplaced[:10])
            )

        table = pd.DataFrame(
            [
                {
                    "hops upstream": depths[uid],
                    "type": normalize_type(nodes[uid]),
                    "name": nodes[uid].get("name"),
                    "unique_id": uid,
                    "materialization": nodes[uid].get("materializedType"),
                    "last run status": (nodes[uid].get("executionInfo") or {}).get(
                        "lastRunStatus"
                    ),
                    "last built / loaded": (
                        (nodes[uid].get("executionInfo") or {}).get("executeCompletedAt")
                        or (nodes[uid].get("freshness") or {}).get("maxLoadedAt")
                    ),
                }
                for uid in keep
            ]
        ).sort_values(["hops upstream", "type", "name"])

        if show_table:
            with st.expander("Upstream nodes as a table"):
                st.dataframe(table, use_container_width=True, hide_index=True)
                st.download_button(
                    "Download lineage (CSV)",
                    table.to_csv(index=False).encode(),
                    file_name=f"{name}_upstream.csv",
                    mime="text/csv",
                    key=f"{key_prefix}_dl_lineage",
                )

        return {
            "unique_id": unique_id,
            "name": name,
            "nodes_shown": len(keep),
            "max_depth": max_depth,
            "counts": counts.to_dict(),
            "unplaced": unplaced,
            "table": table,
            "is_root": False,
        }


# ---------------------------------------------------------------------------
# Tile 2 — compiled SQL
# ---------------------------------------------------------------------------


def render_sql_tile(
    model: str,
    cfg: Optional[DbtConfig] = None,
    container: Any = None,
    key_prefix: str = DEFAULT_KEY_PREFIX,
    heading: Optional[str] = None,
    show_metrics: bool = True,
    show_download: bool = True,
    show_preview: bool = True,
    **cfg_kwargs: Any,
) -> Optional[str]:
    """Compiled SQL for `model`, as dbt executed it in Snowflake.

    show_preview  Adds a 'preview the built relation' expander using the host
                  app's active Snowpark session. Set False if your app already
                  offers data preview, or to avoid extra warehouse spend.

    Returns the compiled SQL string, or None if unavailable.
    """
    with _container(container):
        try:
            cfg = cfg or get_config(**cfg_kwargs)
            unique_id = _resolve_model(cfg, model)
            node = _fetch_compiled_sql(
                cfg.metadata_url, cfg.token, cfg.environment_id, unique_id
            )
        except Exception as exc:  # noqa: BLE001
            _handle(exc, "Compiled SQL query")
            return None

        st.subheader(heading if heading is not None else "Compiled SQL")

        info = node.get("executionInfo") or {}
        if show_metrics:
            m = st.columns(4)
            m[0].metric("Relation", f"{node['schema']}.{node['alias']}")
            m[1].metric("Materialization", node.get("materializedType") or "—")
            m[2].metric("Last run", info.get("lastRunStatus") or "—")
            m[3].metric(
                "Build time",
                f"{info['executionTime']:.1f}s" if info.get("executionTime") else "—",
            )
        st.caption(
            f"`{node['database']}.{node['schema']}.{node['alias']}`  ·  "
            f"last built {info.get('executeCompletedAt') or 'never'}  ·  "
            f"run {info.get('lastRunId') or '—'}"
        )

        if info.get("lastRunError"):
            st.error(f"Last run error: {info['lastRunError']}")

        compiled = node.get("compiledCode")
        if not compiled:
            st.warning(
                "No compiled SQL available. Expected for Python models, or when "
                "the environment has no successful run with compiled artifacts yet."
            )
            return None

        st.code(compiled, language="sql")

        if show_download:
            st.download_button(
                "Download .sql",
                compiled.encode(),
                file_name=f"{node['name']}_compiled.sql",
                mime="text/plain",
                key=f"{key_prefix}_dl_sql",
            )

        if show_preview:
            with st.expander("Preview the built relation"):
                st.caption(
                    "Queries the table dbt built — not a re-execution of the SQL. "
                    "Uses this app's Snowpark session and warehouse."
                )
                limit = st.number_input(
                    "Rows",
                    min_value=1,
                    max_value=1000,
                    value=100,
                    step=50,
                    key=f"{key_prefix}_limit",
                )
                if st.button("Run preview", key=f"{key_prefix}_preview"):
                    try:
                        from snowflake.snowpark.context import get_active_session

                        session = get_active_session()
                        fqn = (
                            f'"{node["database"]}"."{node["schema"]}"."{node["alias"]}"'
                        )
                        st.dataframe(
                            session.sql(
                                f"select * from {fqn} limit {int(limit)}"
                            ).to_pandas(),
                            use_container_width=True,
                        )
                    except ImportError:
                        st.info(
                            "Snowpark is not available — expected when running "
                            "locally. The tiles above still work."
                        )
                    except Exception as exc:  # noqa: BLE001
                        st.error(f"Preview failed: {exc}")

        return compiled
