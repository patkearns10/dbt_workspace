import os
import requests
import json
from datetime import datetime, timezone, timedelta
import sys

# Create a timestamped log file name
log_filename = f"semantic_layer_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

# Duplicate stdout so print() writes to both terminal and log
class Tee:
    def __init__(self, *files):
        self.files = files
    def write(self, obj):
        for f in self.files:
            f.write(obj)
            f.flush()  # flush immediately
    def flush(self):
        for f in self.files:
            f.flush()

sys.stdout = Tee(sys.stdout, open(log_filename, "w"))

# -------------------------------
# Environment Variables
# -------------------------------
DBT_API_KEY        = os.environ.get("DBT_API_KEY")
DBT_ACCOUNT_ID     = os.environ.get("DBT_ACCOUNT_ID")
DBT_ENVIRONMENT_ID = os.environ.get("DBT_ENVIRONMENT_ID")

if not all([DBT_API_KEY, DBT_ACCOUNT_ID, DBT_ENVIRONMENT_ID]):
    raise EnvironmentError("Missing one or more required env vars: DBT_API_KEY, DBT_ACCOUNT_ID, DBT_ENVIRONMENT_ID")

# Optional date filters (UTC)
START_DATE = os.environ.get("START_DATE")
END_DATE = os.environ.get("END_DATE")

# Default to last 7 days if not set
if START_DATE:
    start_dt = datetime.fromisoformat(START_DATE).replace(tzinfo=timezone.utc)
else:
    start_dt = datetime.now(timezone.utc) - timedelta(days=900)

if END_DATE:
    end_dt = datetime.fromisoformat(END_DATE).replace(tzinfo=timezone.utc)
else:
    end_dt = datetime.now(timezone.utc)

# -------------------------------
# GraphQL Endpoint & Query
# -------------------------------

# Change this to be your semantic layer graphql endpoint following these directions: 
# https://docs.getdbt.com/docs/dbt-cloud-apis/sl-graphql#dbt-semantic-layer-graphql-api
url = f"https://tk626.semantic-layer.us1.dbt.com/api/graphql"

headers = {
    "Authorization": f"Bearer {DBT_API_KEY}",
    "Content-Type": "application/json",
}

query = """
query GetQueryRecords($environmentId: BigInt!, $pageNum: Int) {
  queryRecords(
    environmentId: $environmentId,
    pageNum: $pageNum
  ) {
    items {
      queryId
      status
      startTime
      endTime
      sqlDialect
      connectionSchema
      error
      queryDetails {
        ... on SemanticLayerQueryDetails {
          params {
            type
            metrics { name }
            groupBy { name grain }
            where { sql }
          }
        }
      }
    }
    totalItems
    pageNum
    pageSize
  }
}
"""


def fetch_all_query_records():
    all_records = []
    page = 1
    while True:
        response = requests.post(
            url,
            headers=headers,
            json={"query": query, "variables": {"environmentId": DBT_ENVIRONMENT_ID, "pageNum": page}},
        )

        try:
            data = response.json()
        except Exception as e:
            print("⚠️ Could not parse response as JSON:", response.text)
            raise e

        if "data" not in data or data["data"] is None:
            print("❌ GraphQL error response:")
            print(json.dumps(data, indent=2))
            raise Exception("GraphQL query failed or returned no data")

        records = data["data"]["queryRecords"]["items"]
        all_records.extend(records)

        total_items = data["data"]["queryRecords"]["totalItems"]
        page_size = data["data"]["queryRecords"]["pageSize"]

        if page * page_size >= total_items:
            break
        page += 1

    return all_records


def parse_and_display(records):
    filtered = []
    sl_errors = []
    for r in records:
        # Parse time
        try:
            start_time = datetime.fromisoformat(r["startTime"].replace("Z", "+00:00"))
        except Exception:
            continue

        # Filter by date and error
        if not (start_dt <= start_time <= end_dt):
            continue
        if not (r.get("error") or r.get("status") == "FAILED"):
            continue
        filtered.append(r)
    print(f"✅ Found {len(filtered)} Warehouse errors on queries between {start_dt} and {end_dt}\n")

    for r in filtered:
        print("=" * 80)
        print(f"Query ID: {r.get('queryId')}")
        print(f"Status: {r.get('status')}")
        print(f"Start Time: {r.get('startTime')}")
        print(f"End Time: {r.get('endTime')}")
        print(f"SQL Dialect: {r.get('sqlDialect')}")
        print(f"Connection: {r.get('connectionSchema')}")
        if (
            r.get("queryDetails")
            and isinstance(r["queryDetails"], dict)
            and "params" in r["queryDetails"]
        ):
            params = r["queryDetails"]["params"]
            metrics = [m["name"] for m in params.get("metrics", [])]
            group_by = params.get("groupBy") or []
            where_clauses = params.get("where") or []

            print(f"Metrics: {metrics}")
            if group_by:
                print(f"Group By: {[g['name'] for g in group_by if g]}")
            if where_clauses:
                print(f"Where: {[w['sql'] for w in where_clauses if w.get('sql')]}")
                print("=" * 80)
                print()
        print("-" * 80)
        if r.get("error"):
            print(f"⚠️ Error: {r['error']}")
        print(' ')

if __name__ == "__main__":
    records = fetch_all_query_records()
    print(f"Fetched {len(records)} total records from Semantic Layer.")
    parse_and_display(records)
