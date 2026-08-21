#!/usr/bin/env python3
"""
Pinpoint a 400 Bad Request from the dbt Discovery API lineage query.

A 400 means the request arrived and the *query* was rejected — so networking,
auth, the URL, and the environment ID are all fine. The cause is a field,
fragment, or filter this deployment's schema does not accept. That specific
reason is in the response body, which the app previously swallowed.

This script does two things:

  1. Prints the raw error body for the full lineage query
  2. Bisects — runs the query as a series of progressively richer variants and
     reports the first one that fails, which names the offending field
  3. Introspects the live schema to confirm what IS available

Usage:
    export DBT_METADATA_URL="https://tk626.metadata.us1.dbt.com/graphql"
    export DBT_SERVICE_TOKEN="dbtc_..."
    export DBT_ENVIRONMENT_ID="12345"

    python diagnose_lineage.py                # uses the first model it finds
    python diagnose_lineage.py fct_orders     # uses a specific model
"""

import json
import os
import sys
from typing import Any, Dict, Optional, Tuple

import requests

URL = os.environ.get("DBT_METADATA_URL")
TOKEN = os.environ.get("DBT_SERVICE_TOKEN")
ENV_ID = os.environ.get("DBT_ENVIRONMENT_ID")

for var, val in (
    ("DBT_METADATA_URL", URL),
    ("DBT_SERVICE_TOKEN", TOKEN),
    ("DBT_ENVIRONMENT_ID", ENV_ID),
):
    if not val:
        sys.exit(f"{var} is not set. See INTEGRATION.md.")

ENV_ID = int(ENV_ID)


def post(query: str, variables: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
    """Return (status_code, parsed_body). Never raises on HTTP status."""
    resp = requests.post(
        URL,
        headers={"authorization": f"Bearer {TOKEN}", "content-type": "application/json"},
        json={"query": query, "variables": variables},
        timeout=60,
    )
    try:
        return resp.status_code, resp.json()
    except ValueError:
        return resp.status_code, {"_raw": resp.text[:4000]}


def errors_of(body: Dict[str, Any]) -> list:
    return body.get("errors") or []


def describe(status: int, body: Dict[str, Any]) -> str:
    errs = errors_of(body)
    if errs:
        return " | ".join(str(e.get("message", e))[:300] for e in errs)
    if "_raw" in body:
        return body["_raw"][:300]
    return f"HTTP {status}"


# ---------------------------------------------------------------------------
# Pick a model to test with
# ---------------------------------------------------------------------------

print("\ndbt Discovery API — lineage query diagnosis")
print("=" * 74)
print(f"endpoint    {URL}")
print(f"environment {ENV_ID}")

target = sys.argv[1] if len(sys.argv) > 1 else None

status, body = post(
    """
    query Models($environmentId: BigInt!, $first: Int!) {
      environment(id: $environmentId) {
        applied { models(first: $first) { edges { node { uniqueId name } } } }
      }
    }
    """,
    {"environmentId": ENV_ID, "first": 500},
)
if status != 200 or errors_of(body):
    sys.exit(
        f"\nThe basic model list query already fails, so this is not lineage-"
        f"specific:\n  {describe(status, body)}\n"
    )

all_models = [e["node"] for e in body["data"]["environment"]["applied"]["models"]["edges"]]
if target:
    match = [m for m in all_models if m["name"] == target]
    if not match:
        sys.exit(f"No model named '{target}' in the first 500 models.")
    node = match[0]
else:
    node = all_models[0]

uid = node["uniqueId"]
print(f"test model  {node['name']}  ({uid})")
print(f"model list  OK — {len(all_models)} models returned\n")

VARS = {"environmentId": ENV_ID, "uniqueId": uid}


# ---------------------------------------------------------------------------
# Part 1 — bisect the ancestors query
# ---------------------------------------------------------------------------


def ancestors_query(fragments: str) -> str:
    return """
    query A($environmentId: BigInt!, $uniqueId: String!) {
      environment(id: $environmentId) {
        applied {
          models(first: 1, filter: { uniqueIds: [$uniqueId] }) {
            edges { node { uniqueId name ancestors(types: [Model, Source, Seed, Snapshot]) { %s } } }
          }
        }
      }
    }
    """ % fragments


# Ordered simplest -> richest. The first failure names the culprit.
STEPS = [
    (
        "filter by uniqueIds (no ancestors)",
        """
        query F($environmentId: BigInt!, $uniqueId: String!) {
          environment(id: $environmentId) {
            applied {
              models(first: 1, filter: { uniqueIds: [$uniqueId] }) {
                edges { node { uniqueId name } }
              }
            }
          }
        }
        """,
        VARS,
    ),
    (
        "[control, expected to FAIL] ancestors() with no types argument",
        """
        query A0($environmentId: BigInt!, $uniqueId: String!) {
          environment(id: $environmentId) {
            applied {
              models(first: 1, filter: { uniqueIds: [$uniqueId] }) {
                edges { node { ancestors { ... on ModelAppliedStateNestedNode { uniqueId } } } }
              }
            }
          }
        }
        """,
        VARS,
    ),
    (
        "ancestors(types:) enum values accepted",
        ancestors_query("... on ModelAppliedStateNestedNode { uniqueId }"),
        VARS,
    ),
    (
        "Source fragment + sourceName",
        ancestors_query(
            "... on ModelAppliedStateNestedNode { uniqueId } "
            "... on SourceAppliedStateNestedNode { uniqueId name sourceName }"
        ),
        VARS,
    ),
    (
        "Seed + Snapshot fragments",
        ancestors_query(
            "... on ModelAppliedStateNestedNode { uniqueId } "
            "... on SeedAppliedStateNestedNode { uniqueId name } "
            "... on SnapshotAppliedStateNestedNode { uniqueId name }"
        ),
        VARS,
    ),
    (
        "resourceType on nested nodes",
        ancestors_query("... on ModelAppliedStateNestedNode { uniqueId resourceType }"),
        VARS,
    ),
    (
        "materializedType on Model nested node",
        ancestors_query(
            "... on ModelAppliedStateNestedNode { uniqueId materializedType }"
        ),
        VARS,
    ),
    (
        "executionInfo on Model nested node",
        ancestors_query(
            "... on ModelAppliedStateNestedNode { uniqueId "
            "executionInfo { lastRunStatus executeCompletedAt } }"
        ),
        VARS,
    ),
    (
        "freshness.maxLoadedAt on Source nested node",
        ancestors_query(
            "... on SourceAppliedStateNestedNode { uniqueId freshness { maxLoadedAt } }"
        ),
        VARS,
    ),
    (
        "freshness.freshnessStatus on Source nested node",
        ancestors_query(
            "... on SourceAppliedStateNestedNode { uniqueId "
            "freshness { maxLoadedAt freshnessStatus } }"
        ),
        VARS,
    ),
    (
        "[control, expected to FAIL] executionInfo unaliased on 2 fragments",
        ancestors_query(
            "... on ModelAppliedStateNestedNode { uniqueId "
            "executionInfo { lastRunStatus } } "
            "... on SeedAppliedStateNestedNode { uniqueId "
            "executionInfo { lastRunStatus } }"
        ),
        VARS,
    ),
    (
        "executionInfo ALIASED per fragment  [the fix]",
        ancestors_query(
            "... on ModelAppliedStateNestedNode { uniqueId "
            "modelExecutionInfo: executionInfo { lastRunStatus } } "
            "... on SnapshotAppliedStateNestedNode { uniqueId "
            "snapshotExecutionInfo: executionInfo { lastRunStatus } } "
            "... on SeedAppliedStateNestedNode { uniqueId "
            "seedExecutionInfo: executionInfo { lastRunStatus } }"
        ),
        VARS,
    ),
    (
        "models(filter:) + parents  [edge reconstruction]",
        """
        query MP($environmentId: BigInt!, $uniqueIds: [String!], $first: Int!) {
          environment(id: $environmentId) {
            applied {
              models(first: $first, filter: { uniqueIds: $uniqueIds }) {
                edges { node { uniqueId parents { uniqueId name resourceType } } }
              }
            }
          }
        }
        """,
        {"environmentId": ENV_ID, "uniqueIds": [uid], "first": 100},
    ),
    (
        "snapshots(filter: { uniqueIds: }) + parents",
        """
        query SP($environmentId: BigInt!, $uniqueIds: [String!], $first: Int!) {
          environment(id: $environmentId) {
            applied {
              snapshots(first: $first, filter: { uniqueIds: $uniqueIds }) {
                edges { node { uniqueId parents { uniqueId name resourceType } } }
              }
            }
          }
        }
        """,
        {"environmentId": ENV_ID, "uniqueIds": [uid], "first": 100},
    ),
]

print("-" * 74)
print("PART 1 — BISECT: is any single field or filter rejected?")
print("-" * 74)

failures = []
for label, query, variables in STEPS:
    status, body = post(query, variables)
    expected_fail = label.startswith("[control")
    if status == 200 and not errors_of(body):
        print(f"  ok    {label}")
    elif expected_fail:
        # This one SHOULD fail — it proves the bisect can detect a rejection.
        print(f"  FAIL  {label}  <- as designed, ignore")
        print(f"        -> {describe(status, body)}")
    else:
        print(f"  FAIL  {label}")
        print(f"        -> {describe(status, body)}")
        failures.append(label)


# ---------------------------------------------------------------------------
# Part 1b — reproduce the app at real scale
# ---------------------------------------------------------------------------
# The bisect above uses one model and one uniqueId. The app asks for `parents`
# on the whole ancestor set at once, and the docs call nested nodes like
# `parents` among the most complex fields. A query that is valid in miniature
# can still be rejected at scale, which is exactly what a 400 looks like.

print()
print("-" * 74)
print("PART 2 — SCALE: same queries, real ancestor counts")
print("-" * 74)

FULL_ANCESTORS = """
query Ancestors($environmentId: BigInt!, $uniqueId: String!) {
  environment(id: $environmentId) {
    applied {
      models(first: 1, filter: { uniqueIds: [$uniqueId] }) {
        edges { node { uniqueId name
          ancestors(types: [Model, Source, Seed, Snapshot]) {
            ... on ModelAppliedStateNestedNode {
              uniqueId name resourceType materializedType
              modelExecutionInfo: executionInfo { lastRunStatus executeCompletedAt } }
            ... on SourceAppliedStateNestedNode {
              uniqueId name sourceName resourceType
              freshness { maxLoadedAt freshnessStatus } }
            ... on SnapshotAppliedStateNestedNode {
              uniqueId name resourceType
              snapshotExecutionInfo: executionInfo { lastRunStatus executeCompletedAt } }
            ... on SeedAppliedStateNestedNode {
              uniqueId name resourceType
              seedExecutionInfo: executionInfo { lastRunStatus executeCompletedAt } }
          } } }
      }
    }
  }
}
"""

status, body = post(FULL_ANCESTORS, VARS)
if status != 200 or errors_of(body):
    print("  FAIL  full combined ancestors query (aliased, all fragments)")
    print(f"        -> {describe(status, body)}")
    failures.append("full combined ancestors query")
    ancestors = []
else:
    ancestors = (
        body["data"]["environment"]["applied"]["models"]["edges"][0]["node"]["ancestors"]
        or []
    )
    print(f"  ok    full combined ancestors query (aliased) — {len(ancestors)} ancestors")

resolvable = [
    a["uniqueId"]
    for a in ancestors
    if a.get("uniqueId", "").startswith(("model.", "snapshot."))
] + [uid]

MODEL_PARENTS = """
query MP($environmentId: BigInt!, $uniqueIds: [String!], $first: Int!) {
  environment(id: $environmentId) {
    applied {
      models(first: $first, filter: { uniqueIds: $uniqueIds }) {
        edges { node { uniqueId parents { uniqueId name resourceType } } }
      }
    }
  }
}
"""

need = len(resolvable)
print(f"        {need} node(s) need parent edges")
print("        largest batch this endpoint accepts (descending):")

# Always try the FULL set first, then step down. Clamping to `need` and
# de-duplicating matters: without it, a small ancestor set makes the probe skip
# every size above the list length and then report the first size it happened
# to try as though it were a ceiling.
candidates = sorted({min(s, need) for s in (500, 200, 100, 50, 25, 10, 5, 1)}, reverse=True)

largest_ok = 0
for size in candidates:
    batch = resolvable[:size]
    status, body = post(
        MODEL_PARENTS,
        {"environmentId": ENV_ID, "uniqueIds": batch, "first": max(len(batch), 1)},
    )
    if status == 200 and not errors_of(body):
        print(f"          ok    batch of {size:>3}")
        largest_ok = size
        break
    print(f"          FAIL  batch of {size:>3} -> {describe(status, body)[:120]}")

if largest_ok == 0:
    print("        -> rejected at every batch size, including 1.")
    failures.append("parent-edge query at every batch size")
elif largest_ok >= need:
    print(
        f"        -> the full set of {need} was accepted in one request. No batch\n"
        f"           ceiling found for this model; batching is not the issue here."
    )
else:
    print(
        f"        -> ceiling is between {largest_ok} and the full {need}.\n"
        f"           dbt_lineage.EDGE_CHUNK is {50} and halves on rejection, so\n"
        f"           this is handled — but tell me the number above."
    )

# ---------------------------------------------------------------------------
# Part 2 — schema introspection, to show what IS valid
# ---------------------------------------------------------------------------

print()
print("-" * 74)
print("SCHEMA — what this deployment actually accepts")
print("-" * 74)

INTROSPECT = """
query Introspect($name: String!) {
  __type(name: $name) {
    name
    kind
    inputFields { name type { name kind ofType { name kind } } }
    fields { name type { name kind ofType { name kind } } }
    enumValues { name }
  }
}
"""


def show_type(name: str, limit: int = 40) -> None:
    status, body = post(INTROSPECT, {"name": name})
    t = (body.get("data") or {}).get("__type")
    if not t:
        print(f"\n  {name}: not present in this schema")
        return
    print(f"\n  {name}  ({t['kind']})")
    for bucket in ("inputFields", "fields", "enumValues"):
        items = t.get(bucket) or []
        if items:
            names = sorted(i["name"] for i in items)
            shown = ", ".join(names[:limit])
            more = f"  … +{len(names) - limit} more" if len(names) > limit else ""
            print(f"    {bucket}: {shown}{more}")


for type_name in (
    "ModelAppliedFilter",
    "SnapshotAppliedFilter",
    "ModelAppliedStateNestedNode",
    "SourceAppliedStateNestedNode",
    "SnapshotAppliedStateNestedNode",
    "SeedAppliedStateNestedNode",
    "SourceFreshness",
    "AncestorNodeType",
):
    show_type(type_name)

# ---------------------------------------------------------------------------

print()
print("=" * 74)
if not failures:
    print("No real failures for this model. If the app still 400s, the model you")
    print("had selected there is probably not this one — the app's error names the")
    print("tile, not the model. Re-run against that exact model:")
    print("    python diagnose_lineage.py <the_model_selected_in_the_app>")
    print()
    print("If batching was required above, upgrade dbt_lineage.py and retry — the")
    print("module now chunks parent-edge queries at 50 and halves on rejection.")
else:
    print(f"{len(failures)} real failure(s):")
    for f in failures:
        print(f"    - {f}")
    print()
    print("Cross-check against the SCHEMA section above, then send me the output.")
print()
