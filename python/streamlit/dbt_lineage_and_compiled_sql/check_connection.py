#!/usr/bin/env python3
"""
Pre-flight check for the dbt Discovery API — no Streamlit required.

Run this FIRST when testing locally. It validates your URL, token, and
environment ID in isolation, so if something is wrong you find out here
instead of debugging it through the Streamlit UI.

Usage:
    export DBT_METADATA_URL="https://abc123.metadata.us1.dbt.com/graphql"
    export DBT_SERVICE_TOKEN="dbtc_..."
    export DBT_ENVIRONMENT_ID="12345"

    python check_connection.py                  # connectivity + model count
    python check_connection.py stg_orders       # also test lineage + SQL for one model
"""

import json
import os
import sys

import requests

URL = os.environ.get("DBT_METADATA_URL")
TOKEN = os.environ.get("DBT_SERVICE_TOKEN")
ENV_ID = os.environ.get("DBT_ENVIRONMENT_ID")


def fail(msg: str) -> None:
    print(f"  FAIL  {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"  ok    {msg}")


def query(gql: str, variables: dict) -> dict:
    resp = requests.post(
        URL,
        headers={"authorization": f"Bearer {TOKEN}", "content-type": "application/json"},
        json={"query": gql, "variables": variables},
        timeout=60,
    )
    if resp.status_code == 401:
        fail(
            "401 Unauthorized. The token is wrong, expired, or lacks Discovery API "
            "access. Generate a 'Metadata Only' service token in dbt platform."
        )
    if resp.status_code == 403:
        fail(
            "403 Forbidden. dbt platform IP restrictions are blocking this request. "
            "Add your current public IP to the allowlist (Account settings -> IP "
            "restrictions), or test from an allowlisted network."
        )
    if resp.status_code == 404:
        fail(f"404 from {URL}. Check the URL — it must end in /graphql.")
    resp.raise_for_status()

    payload = resp.json()
    if payload.get("errors"):
        fail("GraphQL errors:\n" + json.dumps(payload["errors"], indent=2))
    return payload["data"]


# ---------------------------------------------------------------------------

print("\ndbt Discovery API pre-flight check")
print("-" * 60)

if not URL:
    fail("DBT_METADATA_URL is not set.")
if not TOKEN:
    fail("DBT_SERVICE_TOKEN is not set.")
if not ENV_ID:
    fail("DBT_ENVIRONMENT_ID is not set.")
if not URL.rstrip("/").endswith("/graphql"):
    fail(f"DBT_METADATA_URL should end in /graphql — got {URL}")

env_id = int(ENV_ID)
ok(f"config looks sane  ({URL}, environment {env_id})")

# --- 1. Can we reach it and is the token good? ------------------------------

data = query(
    """
    query Ping($environmentId: BigInt!) {
      environment(id: $environmentId) {
        applied { models(first: 1) { totalCount } }
      }
    }
    """,
    {"environmentId": env_id},
)
env = data.get("environment")
if not env:
    fail(
        f"Environment {env_id} returned null. Either the ID is wrong, or this token "
        "does not have access to that environment's project."
    )

total = env["applied"]["models"]["totalCount"]
if total == 0:
    fail(
        f"Environment {env_id} is reachable but has 0 models. The environment needs "
        "at least one successful job run before metadata appears."
    )
ok(f"authenticated — {total:,} models in environment {env_id}")

# --- 2. Optional: exercise the two queries the app depends on ---------------

target_name = sys.argv[1] if len(sys.argv) > 1 else None
if not target_name:
    print("-" * 60)
    print("Connectivity is good. Pass a model name to also test lineage + SQL:")
    print("    python check_connection.py my_model_name\n")
    sys.exit(0)

data = query(
    """
    query FindModel($environmentId: BigInt!, $first: Int!) {
      environment(id: $environmentId) {
        applied {
          models(first: $first) { edges { node { uniqueId name } } }
        }
      }
    }
    """,
    {"environmentId": env_id, "first": 500},
)
matches = [
    n["node"]
    for n in data["environment"]["applied"]["models"]["edges"]
    if n["node"]["name"] == target_name
]
if not matches:
    fail(
        f"No model named '{target_name}' in the first 500 models. "
        "Check the spelling, or just run without an argument."
    )
uid = matches[0]["uniqueId"]
ok(f"found model  {uid}")

# Lineage — the same query the DAG tile uses
data = query(
    """
    query Ancestors($environmentId: BigInt!, $uniqueId: String!) {
      environment(id: $environmentId) {
        applied {
          models(first: 1, filter: { uniqueIds: [$uniqueId] }) {
            edges { node {
              ancestors(types: [Model, Source, Seed, Snapshot]) {
                ... on ModelAppliedStateNestedNode  { uniqueId }
                ... on SourceAppliedStateNestedNode { uniqueId }
                ... on SeedAppliedStateNestedNode   { uniqueId }
                ... on SnapshotAppliedStateNestedNode { uniqueId }
              }
            } }
          }
        }
      }
    }
    """,
    {"environmentId": env_id, "uniqueId": uid},
)
ancestors = data["environment"]["applied"]["models"]["edges"][0]["node"]["ancestors"]
by_type: dict = {}
for a in ancestors:
    kind = a["uniqueId"].split(".")[0]
    by_type[kind] = by_type.get(kind, 0) + 1
summary = ", ".join(f"{v} {k}" for k, v in sorted(by_type.items())) or "none"
ok(f"lineage: +{target_name} resolves {len(ancestors)} upstream nodes ({summary})")

# Compiled SQL — the same query the SQL tile uses
data = query(
    """
    query CompiledSql($environmentId: BigInt!, $uniqueId: String!) {
      environment(id: $environmentId) {
        applied {
          models(first: 1, filter: { uniqueIds: [$uniqueId] }) {
            edges { node {
              database schema alias materializedType compiledCode
              executionInfo { lastRunStatus executeCompletedAt }
            } }
          }
        }
      }
    }
    """,
    {"environmentId": env_id, "uniqueId": uid},
)
node = data["environment"]["applied"]["models"]["edges"][0]["node"]
code = node.get("compiledCode")
if not code:
    print(
        "  warn  compiledCode is empty. Normal for Python models; otherwise the "
        "environment may not have a successful run with compiled artifacts."
    )
else:
    ok(
        f"compiled SQL: {len(code.splitlines())} lines  "
        f"-> {node['database']}.{node['schema']}.{node['alias']} "
        f"({node.get('materializedType')}, last run {(node.get('executionInfo') or {}).get('lastRunStatus')})"
    )

print("-" * 60)
print("All checks passed. Now run:  streamlit run streamlit_app.py\n")
