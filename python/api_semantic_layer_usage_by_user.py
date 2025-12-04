import os
import requests
import json
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import sys

# Create a timestamped log file name
log_filename = f"semantic_layer_usage_by_user_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

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
    start_dt = datetime.now(timezone.utc) - timedelta(days=7)

if END_DATE:
    end_dt = datetime.fromisoformat(END_DATE).replace(tzinfo=timezone.utc)
else:
    end_dt = datetime.now(timezone.utc)

# -------------------------------
# API Endpoints & Headers
# -------------------------------

# Change this to be your semantic layer graphql endpoint following these directions: 
# https://docs.getdbt.com/docs/dbt-cloud-apis/sl-graphql#dbt-semantic-layer-graphql-api
graphql_url = f"https://tk626.semantic-layer.us1.dbt.com/api/graphql"

# dbt Cloud API v2 endpoint for user details
# https://docs.getdbt.com/dbt-cloud/api-v2#/operations/List%20Users
api_v2_url = f"https://cloud.getdbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/users/"

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
      queriedByUserId
      queryDetails {
        ... on SemanticLayerQueryDetails {
          params {
            type
            metrics {
              name
            }
            groupBy {
              name
              grain
            }
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


def fetch_users():
    """
    Fetch all users from dbt Cloud API v2.
    Returns a dictionary mapping user_id to user details.
    """
    print("👥 Fetching user details from dbt Cloud API...")
    
    try:
        response = requests.get(api_v2_url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        users = {}
        
        for user in data.get("data", []):
            user_id = str(user.get("id"))
            users[user_id] = {
                "name": f"{user.get('first_name', '')} {user.get('last_name', '')}".strip() or user.get("email", "Unknown"),
                "email": user.get("email", ""),
            }
        
        print(f"✅ Fetched {len(users)} users from dbt Cloud")
        if len(users) > 0 and len(users) <= 10:
            print(f"   User IDs in dbt Cloud: {', '.join(sorted(users.keys()))}")
        print()
        return users
    
    except Exception as e:
        print(f"⚠️ Warning: Could not fetch users from dbt Cloud API: {e}")
        print("   Continuing with user IDs only...\n")
        return {}


def fetch_all_query_records():
    all_records = []
    page = 1
    while True:
        response = requests.post(
            graphql_url,
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

        print(f"📄 Fetched page {page} ({len(records)} records)")

        if page * page_size >= total_items:
            break
        page += 1

    return all_records


def analyze_by_user(records):
    """
    Analyze queries by user ID and metrics queried.
    Returns a dictionary with user statistics.
    """
    user_stats = defaultdict(lambda: {
        "total_queries": 0,
        "successful_queries": 0,
        "failed_queries": 0,
        "metrics_used": defaultdict(int),  # metric_name -> count
        "earliest_query": None,
        "latest_query": None,
        "query_types": defaultdict(int),  # type -> count
        "dimensions_used": set(),
    })

    filtered_records = []
    user_ids_found = set()
    
    for r in records:
        # Parse time
        try:
            start_time = datetime.fromisoformat(r["startTime"].replace("Z", "+00:00"))
        except Exception:
            continue

        # Filter by date
        if not (start_dt <= start_time <= end_dt):
            continue
        
        filtered_records.append(r)
        
        # Check if queriedByUserId exists and what its value is
        user_id_raw = r.get("queriedByUserId")
        if user_id_raw is not None:
            user_ids_found.add(str(user_id_raw))
        
        user_id = str(user_id_raw) if user_id_raw is not None else "Unknown"
        status = r.get("status", "UNKNOWN")
        
        # Update user stats
        stats = user_stats[user_id]
        stats["total_queries"] += 1
        
        if status == "SUCCESSFUL":
            stats["successful_queries"] += 1
        elif status == "FAILED":
            stats["failed_queries"] += 1
        
        # Track earliest and latest queries
        if stats["earliest_query"] is None or start_time < stats["earliest_query"]:
            stats["earliest_query"] = start_time
        if stats["latest_query"] is None or start_time > stats["latest_query"]:
            stats["latest_query"] = start_time
        
        # Extract metrics and dimensions from query details
        if (
            r.get("queryDetails")
            and isinstance(r["queryDetails"], dict)
            and "params" in r["queryDetails"]
        ):
            params = r["queryDetails"]["params"]
            
            # Track query type
            query_type = params.get("type", "unknown")
            stats["query_types"][query_type] += 1
            
            # Track metrics
            metrics = params.get("metrics", [])
            for metric in metrics:
                if metric and "name" in metric:
                    metric_name = metric["name"]
                    stats["metrics_used"][metric_name] += 1
            
            # Track dimensions
            group_by = params.get("groupBy") or []
            for dim in group_by:
                if dim and "name" in dim:
                    stats["dimensions_used"].add(dim["name"])
    
    # Debug output about user IDs
    if user_ids_found:
        print(f"🔍 Found {len(user_ids_found)} unique user ID(s) in query records:")
        for uid in sorted(user_ids_found):
            print(f"   • User ID: {uid}")
    else:
        print("⚠️  No 'queriedByUserId' field found in any query records (all are None/missing)")
    print()
    
    return dict(user_stats), filtered_records


def display_user_table(user_stats, users_map):
    """
    Display a simple table of users and their query counts.
    """
    print("\n" + "=" * 80)
    print("📊 QUERIES BY USER")
    print(f"Date Range: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}")
    print("=" * 80)
    
    if not user_stats:
        print("\n⚠️  No queries found in the specified date range.\n")
        return
    
    # Check for unmatched user IDs
    unmatched_users = [uid for uid in user_stats.keys() if uid != "Unknown" and uid not in users_map]
    if unmatched_users:
        print(f"\n⚠️  Warning: {len(unmatched_users)} user ID(s) from queries not found in dbt Cloud API:")
        for uid in unmatched_users[:5]:  # Show first 5
            print(f"   • {uid}")
        if len(unmatched_users) > 5:
            print(f"   ... and {len(unmatched_users) - 5} more")
        print()
    
    # Sort users by total queries (descending)
    sorted_users = sorted(user_stats.items(), key=lambda x: x[1]["total_queries"], reverse=True)
    
    # Print table header
    print(f"\n{'User Name':<40} | {'Query Count':>12}")
    print("-" * 80)
    
    # Print each user row
    for user_id, stats in sorted_users:
        # Get user name from the users map, fallback to user_id
        user_name = users_map.get(user_id, {}).get("name", f"User ID: {user_id}")
        query_count = stats['total_queries']
        
        # Truncate long names
        if len(user_name) > 39:
            user_name = user_name[:36] + "..."
        
        print(f"{user_name:<40} | {query_count:>12,}")
    
    print("-" * 80)
    print(f"{'TOTAL':<40} | {sum(s['total_queries'] for s in user_stats.values()):>12,}")
    print()


def display_metrics_table(user_stats):
    """
    Display a simple table of metrics and their query counts.
    """
    print("\n" + "=" * 80)
    print("📊 QUERIES BY METRIC")
    print(f"Date Range: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}")
    print("=" * 80)
    
    if not user_stats:
        print("\n⚠️  No queries found in the specified date range.\n")
        return
    
    # Aggregate metric counts across all users
    metric_totals = defaultdict(int)
    for stats in user_stats.values():
        for metric, count in stats['metrics_used'].items():
            metric_totals[metric] += count
    
    if not metric_totals:
        print("\n⚠️  No metrics found in queries.\n")
        return
    
    # Sort metrics by query count (descending)
    sorted_metrics = sorted(metric_totals.items(), key=lambda x: x[1], reverse=True)
    
    # Print table header
    print(f"\n{'Metric Name':<50} | {'Query Count':>12}")
    print("-" * 80)
    
    # Print each metric row
    for metric_name, count in sorted_metrics:
        # Truncate long names
        display_name = metric_name
        if len(display_name) > 49:
            display_name = display_name[:46] + "..."
        
        print(f"{display_name:<50} | {count:>12,}")
    
    print("-" * 80)
    print(f"{'TOTAL':<50} | {sum(metric_totals.values()):>12,}")
    print()


def display_summary(user_stats):
    """
    Display brief summary statistics.
    """
    print("\n" + "=" * 80)
    print("📊 SUMMARY")
    print("=" * 80)
    
    total_queries = sum(stats['total_queries'] for stats in user_stats.values())
    total_successful = sum(stats['successful_queries'] for stats in user_stats.values())
    total_failed = sum(stats['failed_queries'] for stats in user_stats.values())
    
    # Collect all unique metrics across all users
    all_metrics = set()
    for stats in user_stats.values():
        all_metrics.update(stats['metrics_used'].keys())
    
    print(f"\n📈 Total Queries: {total_queries:,}")
    
    if total_queries > 0:
        print(f"   ✅ Successful: {total_successful:,} ({total_successful/total_queries*100:.1f}%)")
        print(f"   ❌ Failed: {total_failed:,} ({total_failed/total_queries*100:.1f}%)")
    else:
        print(f"   ✅ Successful: {total_successful:,}")
        print(f"   ❌ Failed: {total_failed:,}")
    
    print(f"\n👥 Total Users: {len(user_stats):,}")
    print(f"📊 Unique Metrics: {len(all_metrics):,}")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    print(f"🚀 Starting Semantic Layer Usage Analysis...")
    print(f"📅 Date Range: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}\n")
    
    # Fetch users from dbt Cloud API
    users_map = fetch_users()
    
    # Fetch query records
    records = fetch_all_query_records()
    print(f"\n✅ Fetched {len(records)} total records from Semantic Layer.")
    
    # Analyze by user
    user_stats, filtered_records = analyze_by_user(records)
    print(f"✅ Filtered to {len(filtered_records)} records in date range.\n")
    
    # Display tables
    display_user_table(user_stats, users_map)
    display_metrics_table(user_stats)
    display_summary(user_stats)
    
    print(f"\n💾 Log saved to: {log_filename}")

