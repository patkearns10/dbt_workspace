"""
Export dbt Cost Insights (daily) to CSV via the dbt Discovery GraphQL API.

Pulls the same data that powers the Cost Insights page in the dbt platform UI, for one
environment (test mode) or for every deployment environment in an account.

Output: one CSV row per environment per day.

Environment variables
---------------------
Required
  DBT_API_KEY          Service token (Bearer auth)
  DBT_ACCOUNT_ID       dbt account ID

Connection
  DBT_URL              Admin API host (default https://cloud.getdbt.com), e.g. https://cloud.<name>.getdbt.com
  DBT_METADATA_URL     Discovery API host. Set this explicitly, e.g. https://<prefix>.metadata.<region>.dbt.com
                       (see Account settings > Access URLs). The default only swaps "https://" for
                       "https://metadata." on DBT_URL, which is usually wrong for prefixed/single-tenant accounts.
  DBT_GRAPHQL_PATH     GraphQL path (default /internal/graphql). costInsights is not in the public /graphql schema.

Scope
  DBT_ENVIRONMENT_ID   Run a single environment (test mode). Leave unset to iterate ALL deployment environments.
  DBT_PROJECT_ID       In all-environments mode, limit to one project.

Options
  DBT_BUILDS_ONLY      1/true/yes = only runDate, executionCount, reusedCount, isCostProcessed. Use for accounts
                       without cost configured (cost fields return 0, isCostProcessed=false). Unprocessed days are
                       kept in this mode.
  DBT_LOOKBACK_DAYS    Days of history (default 7; the UI default view is 30)
  DBT_PAGE_SIZE        Max rows per environment (default 100; warns if hit)
  OUTPUT_CSV           Output file (default cost_insights_<UTC timestamp>.csv)

Behavior
  - Full mode drops days where isCostProcessed is false (zero placeholders, cost not calculated yet).
  - No CSV is written if every API call fails (non-zero exit). In all-environments mode, a failing
    environment is skipped and logged; the rest are still written.
  - Environment variables persist in your shell: `unset DBT_ENVIRONMENT_ID DBT_PROJECT_ID DBT_BUILDS_ONLY`
    before an all-environments / full-cost run.

Mapping to the Cost Insights UI
  Total cost                      executionCost
  Total cost reduction            executionCostSaved
  Total cost without optimization executionCost + executionCostSaved
  Total % reduction               executionCostSaved / (executionCost + executionCostSaved)
  Total query run time reduction  executionTimeSaved
  Reused assets                   reusedCount
  Asset builds                    executionCount
  (Column mapping inferred from the UI; verify against one day's values.)

Usage
  export DBT_API_KEY=... DBT_ACCOUNT_ID=... DBT_METADATA_URL=https://<prefix>.metadata.<region>.dbt.com
  DBT_ENVIRONMENT_ID=123 python api_cost_insights_graphql.py     # single environment
  python api_cost_insights_graphql.py                            # all deployment environments
"""
import csv
import os
import time
from datetime import datetime, timezone

import requests
from pprint import pprint

# ------------------------------------------------------------------------------
# get environment variables
# ------------------------------------------------------------------------------
api_base        = os.getenv('DBT_URL', 'https://cloud.getdbt.com')  # Admin API host (default multitenant)
# Discovery/metadata API host. Defaults to swapping the leading "cloud" -> "metadata" on DBT_URL
# (e.g. https://cloud.getdbt.com -> https://metadata.cloud.getdbt.com). Override for single-tenant/custom hosts.
metadata_base   = os.getenv('DBT_METADATA_URL') or api_base.replace('https://', 'https://metadata.', 1)
api_key         = os.environ['DBT_API_KEY']      # no default, error if not provided
account_id      = os.environ['DBT_ACCOUNT_ID']   # no default, error if not provided

# Optional: set to run a single environment (test mode). Leave unset to iterate ALL envs in the account.
environment_id  = os.getenv('DBT_ENVIRONMENT_ID')
# Optional: limit "all envs" mode to one project
project_id      = os.getenv('DBT_PROJECT_ID')

# costInsights is only exposed on the internal schema (same endpoint Apollo uses), not the public /graphql
graphql_path    = os.getenv('DBT_GRAPHQL_PATH', '/internal/graphql')

# Builds-only mode: just run date + build/reuse counts. Use when cost isn't configured for the account
# (cost fields come back as 0 / isCostProcessed=false), so unprocessed days are KEPT instead of filtered out.
builds_only     = os.getenv('DBT_BUILDS_ONLY', '').lower() in ('1', 'true', 'yes')

lookback_days   = int(os.getenv('DBT_LOOKBACK_DAYS', '7'))
page_size       = int(os.getenv('DBT_PAGE_SIZE', '100'))
output_csv      = os.getenv('OUTPUT_CSV', f"cost_insights_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.csv")

print(f"""
Configuration:
api_base: {api_base}
metadata_base: {metadata_base}
graphql_path: {graphql_path}
account_id: {account_id}
project_id: {project_id or '(all)'}
environment_id: {environment_id or '(all environments)'}
lookback_days: {lookback_days}
builds_only: {builds_only}
output_csv: {output_csv}
""")
# ------------------------------------------------------------------------------

COST_FIELDS = [
    'runDate',
    'executionCount',
    'reusedCount',
    'executionTime',
    'executionTimeSaved',
    'executionComputeUnits',
    'executionComputeUnitsSaved',
    'executionCost',
    'executionCostSaved',
    'isCostProcessed',
]
BUILD_FIELDS = ['runDate', 'executionCount', 'reusedCount', 'isCostProcessed']
FIELDS = BUILD_FIELDS if builds_only else COST_FIELDS
CSV_COLUMNS = ['account_id', 'project_id', 'environment_id', 'environment_name'] + FIELDS


def list_environments():
    """
    Admin API v3: list every environment in the account (paginated).
    Only deployment environments are returned, since cost insights relate to job runs.
    """
    headers = {'Authorization': f'Token {api_key}', 'Content-Type': 'application/json'}
    url = f'{api_base}/api/v3/accounts/{account_id}/environments/'
    envs, offset, limit = [], 0, 100

    while True:
        params = {'limit': limit, 'offset': offset}
        if project_id:
            params['project_id'] = project_id
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        body = response.json()
        data = body.get('data', [])
        envs.extend(data)

        total = body.get('extra', {}).get('pagination', {}).get('total_count', len(envs))
        offset += limit
        if not data or offset >= total:
            break

    envs = [e for e in envs if e.get('type') == 'deployment']
    pprint(f"Found {len(envs)} deployment environments")
    return envs


def invoke_dbt_discovery_api(env_id):
    """
    Invokes the dbt Discovery (metadata) GraphQL API to get cost insights for one environment.
    """
    variables_for_query = {
        "environmentId": int(env_id),
        "filter": {"lookbackDays": lookback_days},
        "first": page_size,
    }

    node_fields = "\n".join(" " * 18 + f for f in FIELDS)
    gql_query = """
        query Cost($environmentId: BigInt!, $filter: CostInsightsFilter, $first: Int) {
          environment(id: $environmentId) {
            costInsights(filter: $filter, first: $first) {
              edges {
                node {
{NODE_FIELDS}
                }
              }
            }
          }
        }
    """.replace("{NODE_FIELDS}", node_fields)

    endpoint_url = f"{metadata_base}{graphql_path}"

    response = requests.post(
        endpoint_url,
        headers={"authorization": "Bearer " + api_key, "content-type": "application/json"},
        json={"query": gql_query, "variables": variables_for_query},
    )
    if not response.ok:
        # surface the GraphQL validation message instead of a bare "400 Bad Request"
        raise RuntimeError(f"HTTP {response.status_code} from {endpoint_url}: {response.text[:1000]}")
    body = response.json()

    # GraphQL returns HTTP 200 even for query errors, so check explicitly
    if body.get('errors'):
        raise RuntimeError(f"GraphQL errors for environment {env_id}: {body['errors']}")

    environment = (body.get('data') or {}).get('environment')
    if not environment:
        return []

    edges = environment['costInsights']['edges']
    if len(edges) >= page_size:
        pprint(f"WARNING: env {env_id} returned {len(edges)} rows (== page size); results may be truncated. "
               f"Raise DBT_PAGE_SIZE or add pagination.")
    return [edge['node'] for edge in edges]


def get_cost_rows(env):
    """Flatten the cost nodes for one environment into CSV-ready dicts."""
    env_id = env['id']
    nodes = invoke_dbt_discovery_api(env_id)
    rows = []
    for node in nodes:
        # skip days where cost hasn't been processed yet (all values are zero placeholders)
        if not builds_only and not node.get('isCostProcessed'):
            continue
        row = {
            'account_id': account_id,
            'project_id': env.get('project_id'),
            'environment_id': env_id,
            'environment_name': env.get('name'),
        }
        row.update({field: node.get(field) for field in FIELDS})
        rows.append(row)
    return rows


def write_csv(rows):
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    pprint(f"Wrote {len(rows)} rows to {output_csv}")


def main():
    try:
        if environment_id:
            # Test mode: single environment, no Admin API lookup needed
            envs = [{'id': environment_id, 'project_id': project_id, 'name': None}]
        else:
            envs = list_environments()

        all_rows = []
        failed_envs = []
        for env in envs:
            try:
                rows = get_cost_rows(env)
                pprint(f"env {env['id']} ({env.get('name')}): {len(rows)} processed days of cost data")
                all_rows.extend(rows)
            except (requests.exceptions.RequestException, RuntimeError) as e:
                # In multi-env mode, one bad environment shouldn't kill the whole run
                pprint(f"Skipping env {env['id']}: {e}")
                failed_envs.append(env['id'])
            time.sleep(0.2)  # be gentle with the API

        if failed_envs:
            pprint(f"API calls failed for environments: {failed_envs}")
        if failed_envs and not all_rows:
            # nothing retrieved: don't write an empty CSV
            raise SystemExit("All API calls failed; no CSV written.")
        write_csv(all_rows)

    except requests.exceptions.RequestException as e:
        pprint(f"Error fetching data from dbt Cloud API: {e}")
        raise
    except Exception as e:
        pprint(f"An unexpected error occurred: {e}")
        raise


if __name__ == "__main__":
    main()
