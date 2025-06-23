"""
Install tabulate (for pretty printing):
# bash
pip install tabulate

You can set your environment variables like this before running:
# bash
export DBT_API_KEY=your_token_here
export ACCOUNT_ID=12345
export ENVIRONMENT_ID_ORIGINAL=370354
export ENVIRONMENT_ID_NEW=222

Run:
python api_get_job_intersection.py

Returns:
  ACCOUNT_ID  JOB_NAME         ENVIRONMENT_ID_ORIGINAL    JOB_ID_ORIGINAL    ENVIRONMENT_ID_NEW    JOB_ID_NEW
------------  -------------  -------------------------  -----------------  --------------------  ------------
       51798  Full Refresh                      370354             882109                 85030        106479
       51798  Nightly Build                     370354             882108                 85030         83771
"""

import requests
import os
from pprint import pprint
from tabulate import tabulate

DBT_API_KEY = os.getenv("DBT_API_KEY")
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
ENVIRONMENT_ID_ORIGINAL = os.getenv("ENVIRONMENT_ID_ORIGINAL")
ENVIRONMENT_ID_NEW = os.getenv("ENVIRONMENT_ID_NEW")

HEADERS = {
    "Authorization": f"Token {DBT_API_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

BASE_URL = "https://cloud.getdbt.com/api/v2"


def get_jobs_for_env(account_id, environment_id):
    """Return a dict of job_name -> job_id for a given environment."""
    jobs = []
    limit = 100
    offset = 0

    while True:
        url = f"{BASE_URL}/accounts/{account_id}/jobs/?limit={limit}&offset={offset}"
        resp = requests.get(url, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

        # Filter jobs by environment_id
        filtered_jobs = [
            job for job in data.get('data', [])
            if str(job.get('environment_id')) == str(environment_id)
        ]
        jobs.extend(filtered_jobs)

        # Pagination control
        pagination = data.get("extra", {}).get("pagination", {})
        total_count = pagination.get("total_count", 0)
        count = pagination.get("count", 0)

        offset += count
        if offset >= total_count:
            break

    return {job['name']: job['id'] for job in jobs}


def main():
    if not all([DBT_API_KEY, ACCOUNT_ID, ENVIRONMENT_ID_ORIGINAL, ENVIRONMENT_ID_NEW]):
        print("Missing one or more required environment variables.")
        return

    original_jobs = get_jobs_for_env(ACCOUNT_ID, ENVIRONMENT_ID_ORIGINAL)
    new_jobs = get_jobs_for_env(ACCOUNT_ID, ENVIRONMENT_ID_NEW)

    intersecting_job_names = set(original_jobs.keys()).intersection(new_jobs.keys())

    rows = []
    for job_name in sorted(intersecting_job_names):
        rows.append([
            ACCOUNT_ID,
            job_name,
            ENVIRONMENT_ID_ORIGINAL,
            original_jobs[job_name],
            ENVIRONMENT_ID_NEW,
            new_jobs[job_name]
        ])

    if rows:
        print(tabulate(rows, headers=[
            "ACCOUNT_ID", "JOB_NAME", "ENVIRONMENT_ID_ORIGINAL",
            "JOB_ID_ORIGINAL", "ENVIRONMENT_ID_NEW", "JOB_ID_NEW"
        ]))
    else:
        print("No matching job names found between environments.")


if __name__ == "__main__":
    main()
