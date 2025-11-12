"""
This script is used to list the model invocations for a given date range.
It uses the dbt Cloud API to fetch the runs and then the run results to get the model invocations.
It then returns a pandas DataFrame with the model invocations.
Example usage: model_run_dataframe = get_model_run_summary(models_list, start_date, end_date)
"""


import requests
import pandas as pd
from datetime import datetime
from collections import defaultdict

# ----------------------------
# CONFIGURATION
# ----------------------------
# TODO: update below Account ID
DBT_CLOUD_ACCOUNT_ID = "1"
# TODO: update below API Token
DBT_CLOUD_API_TOKEN = DBT_CLOUD_API_TOKEN
# TODO: update below URL, this is the base URL for the API
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
            if start_date <= started_at.date() < end_date:
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

    #dedupe just in case
    run_summary = defaultdict(lambda: defaultdict(int))  # {model_name: {(day, hour): count}}

    for run in runs:
        run_id = run["id"]
        run_results = get_run_results(run_id)
        print(f"Checking run {run_id} - {run.get('status')} ({run.get('started_at')})")
        if not run_results:
            continue

        for result in (r for r in run_results.get("results", []) if r["status"] == "success"):
            model_name = result.get("unique_id", "").split(".")[-1]
            if model_name not in model_names:
                continue

            # grab executed at timestamp
            executed_at = datetime.fromisoformat(
                result["timing"][1]["started_at"].replace("Z", "+00:00")
            )
            # Truncate to hour in UTC
            executed_hour = executed_at.replace(minute=0, second=0, microsecond=0)
            # print(f"  Found model: {model_name} @ {executed_hour.isoformat()}")

            # Look up the entry for model_name in run_summary. 
                #If it doesnt exist yet, create a new inner dictionary: defaultdict(int).
            # Inside that inner dictionary, look up executed_hour.
                #If it doesnt exist yet, start with 0.
            # Increment the count by 1.
            run_summary[model_name][executed_hour] += 1

    # Convert to DataFrame
    rows = []
    for model, times in run_summary.items():
        for hour_timestamp, count in times.items():
            rows.append({
                "model_name": model,
                "executed_hour": hour_timestamp.isoformat(), 
                "run_count": count
            })

    # Convert to DataFrame
    if not rows:
        print("⚠️  No matching model runs found in the given date range.")
        print(f"Models searched: {model_names}")
        print(f"Date range: {start_date} → {end_date}")
        return pd.DataFrame(columns=["model_name", "executed_hour", "run_count"])

    df = pd.DataFrame(rows)
    df = df.sort_values(["model_name", "executed_hour"]).reset_index(drop=True)
    return df
