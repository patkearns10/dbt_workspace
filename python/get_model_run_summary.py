import requests
import pandas as pd
from datetime import datetime
from collections import defaultdict

# ----------------------------
# CONFIGURATION
# ----------------------------
DBT_CLOUD_ACCOUNT_ID = "1"
DBT_CLOUD_API_TOKEN = "dbtu_PloSEho21e79DtkBt0nRggI1WB-yjO0LOv5xK3riytDheXLhGY"
JOB_ID = None  # Optional: restrict to one job
BASE_URL = f"https://vu491.us1.dbt.com/api/v2/accounts/{DBT_CLOUD_ACCOUNT_ID}"

# ----------------------------
# FUNCTION TO FETCH RUNS
# ----------------------------
def get_runs(start_date: str, end_date: str):
    """Fetch dbt Cloud job runs between start and end dates"""
    offset=0
    url = f"{BASE_URL}/runs/"
    print(url)
    headers = {"Authorization": f"Token {DBT_CLOUD_API_TOKEN}"}


    all_runs = []
    while True:
        params = {
            "limit": 100,
            "offset": offset,
            "order_by": "id",
            "created_at__range": f"['{start_date}', '{end_date}']",
        }
        response = requests.get(url, headers=headers, params=params)
        if not response.ok:
            print("Error:", response.status_code, response.text)
            response.raise_for_status()
        data = response.json()
        print(response.json()["extra"])
        for run in data["data"]:
            started_at_str = run.get("started_at")
            if not started_at_str:
                continue
            started_at = datetime.fromisoformat(started_at_str.replace("Z", "+00:00"))
            if start_date <= started_at.date() <= end_date:
                all_runs.append(run)

        # pagination control
        pagination = data.get("extra", {}).get("pagination", {})
        total_count = pagination.get("total_count", 0)
        count = pagination.get("count", 0)

        offset += count
        if offset >= total_count:
            break

    return all_runs


# ----------------------------
# FUNCTION TO FETCH RUN RESULTS (ARTIFACTS)
# ----------------------------
def get_run_results(run_id):
    """Fetch run_results.json for a specific run"""
    url = f"{BASE_URL}/runs/{run_id}/artifacts/run_results.json"
    headers = {"Authorization": f"Token {DBT_CLOUD_API_TOKEN}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    return None


# ----------------------------
# MAIN FUNCTION
# ----------------------------
def get_model_run_summary(model_names, start_date, end_date):
    start_date = datetime.fromisoformat(start_date).date()
    end_date = datetime.fromisoformat(end_date).date()

    runs = get_runs(start_date, end_date)
    if not runs:
        print('No runs found!')
    print(f"Runs Found: {len(runs)}")
    run_summary = defaultdict(lambda: defaultdict(int))  # {model_name: {(day, hour): count}}

    for run in runs:
        run_id = run["id"]
        run_results = get_run_results(run_id)
        print(f"Checking run {run_id} - {run.get('status')} ({run.get('started_at')})")
        if not run_results:
            continue

        for result in run_results.get("results", []):
            model_name = result.get("unique_id", "").split(".")[-1]
            if model_name not in model_names:
                continue
            executed_at = datetime.fromisoformat(
                result["timing"][0]["started_at"].replace("Z", "+00:00")
            )
            print(f"  Found model: {model_name} @ {executed_at}")
            day_hour = (executed_at.date(), executed_at.hour)
            run_summary[model_name][day_hour] += 1

    # Convert to DataFrame
    rows = []
    for model, times in run_summary.items():
        for (day, hour), count in times.items():
            rows.append({
                "model_name": model,
                "date": day.isoformat(),
                "hour": hour,
                "run_count": count
            })

    # Convert to DataFrame
    if not rows:
        print("⚠️  No matching model runs found in the given date range.")
        print(f"Models searched: {model_names}")
        print(f"Date range: {start_date} → {end_date}")
        return pd.DataFrame(columns=["model_name", "date", "hour", "run_count"])

    df = pd.DataFrame(rows)
    df = df.sort_values(["model_name", "date", "hour"]).reset_index(drop=True)
    return df


# ----------------------------
# EXAMPLE USAGE
# ----------------------------
if __name__ == "__main__":
    model_list = ["dim_customers", "dim_salesforce_contacts", "fct_consumption_cloud_runs", "fct_coalesce_registrations", "fct_cloud_accounts", "dim_cloud_projects"]
    start_date = "2025-10-07"
    end_date = "2025-10-11"

    df = get_model_run_summary(model_list, start_date, end_date)
    print(df)
