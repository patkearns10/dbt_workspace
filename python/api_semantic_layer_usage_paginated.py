import os
import requests
import json

# -------------------------------
# Environment Variables
# -------------------------------
# Set these in your shell or in a .env file
# export DBT_API_KEY="your-service-token"
# export DBT_ACCOUNT_ID="12345"
# export DBT_ENVIRONMENT_ID="67890"
# optionally export DBT_URL=""

DBT_API_KEY        = os.environ.get("DBT_API_KEY")
DBT_ACCOUNT_ID     = os.environ.get("DBT_ACCOUNT_ID")
DBT_ENVIRONMENT_ID = os.environ.get("DBT_ENVIRONMENT_ID")

if not all([DBT_API_KEY, DBT_ACCOUNT_ID, DBT_ENVIRONMENT_ID]):
    raise EnvironmentError("Missing one or more required env vars: DBT_API_KEY, DBT_ACCOUNT_ID, DBT_ENVIRONMENT_ID")

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
      connectionDetails
      sqlDialect
      connectionSchema
      error
      queryDetails {
        ... on SemanticLayerQueryDetails {
          params {
            type
            metrics { name }
            groupBy { name grain }
            limit
            where { sql }
            orderBy {
              groupBy { name grain }
              metric { name }
              descending
            }
            savedQuery
          }
        }
        ... on RawSqlQueryDetails {
          queryStr
          compiledSql
          numCols
          queryDescription
          queryTitle
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

        # Debug print if something went wrong
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
# -------------------------------
# Parse & Display
# -------------------------------
    for r in records:
        print("=" * 80)
        print(f"Query ID: {r.get('queryId')}")
        print(f"Status: {r.get('status')}")
        print(f"Start Time: {r.get('startTime')}")
        print(f"End Time: {r.get('endTime')}")
        print(f"SQL Dialect: {r.get('sqlDialect')}")
        print(f"Connection: {r.get('connectionSchema')}")
        if r.get("error"):
            print(f"⚠️ Error: {r['error']}")
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


if __name__ == "__main__":
    records = fetch_all_query_records()
    print(f"Fetched {len(records)} query records from Semantic Layer.\n")
    parse_and_display(records)
