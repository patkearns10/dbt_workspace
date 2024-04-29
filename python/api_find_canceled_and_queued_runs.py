"""Look for cancelled and queued jobs, give a rough estimate of utilization based on queue size
poll jobs in past 5 minutes and 60 minutes.
`python find_cancelled_and_queued_jobs.py`
"""
import requests

from datetime import datetime, timedelta
import os
import pandas as pd
from rich import print
import pytz

utc=pytz.UTC

api_key = os.environ['DBT_API_KEY']  # no default here, just throw an error here if key not provided
api_base = os.getenv('DBT_URL', 'https://cloud.getdbt.com') # default to multitenant url
account_id = 51798
BASE_URL = "https://cloud.getdbt.com/api/v2"
QUEUE_SIZE = 500

req_auth_header = {'Authorization': f'Bearer {api_key}','Accept': 'application/json'}

def get_cancelled_runs():
    """Get data from cancelled runs"""
    
    repos_url = f"{api_base}/api/v2/accounts/{account_id}/runs/?include_related=['trigger','job']&status=30&order_by=-id&limit=100"
    # ?include_related=['trigger','job']&status=30&order_by=-id&limit=1000"

    print(repos_url)
    run_job_resp = requests.get(repos_url, headers=req_auth_header)
    # print(run_job_resp.json()["data"])
    return run_job_resp.json()["data"]

def get_queued_runs():
    """Get data from queued runs"""
    repos_url = f"{api_base}/api/v2/accounts/{account_id}/runs/?include_related=['trigger', 'job']&status=1&order_by=-id&limit=100"
    
    print(repos_url)
    run_job_resp = requests.get(repos_url, headers=req_auth_header)
    # print(run_job_resp.json()["data"])
    return run_job_resp.json()["data"]

def harvest_data():
    print(f"Looking for jobs that are cancelled or queued.")
    cancelled_runs = get_cancelled_runs()
    queued_runs = get_queued_runs()
    within_60_minutes = (datetime.utcnow() - timedelta(minutes=60)).replace(tzinfo=utc)
    within_5_minutes = (datetime.utcnow() - timedelta(minutes=5)).replace(tzinfo=utc)
    cancelled_60 = []
    cancelled_5 = []
    queued_60 = []
    queued_5 = []

    for job in cancelled_runs:
        # consider removing  'status_message': 'Cancelled by user.' 
        # if you would like to truly narrow in on system cancellations
        updated_at = pd.to_datetime(job["updated_at"]).replace(tzinfo=utc)
        if updated_at > within_60_minutes:
            cancelled_60.append(job["id"])
        if updated_at > within_5_minutes:
            cancelled_5.append(job["id"])
         
    for job in queued_runs:
        updated_at = pd.to_datetime(job["updated_at"])
        if updated_at > within_60_minutes:
            queued_60.append(job["id"])
        if updated_at > within_5_minutes:
            queued_5.append(job["id"])
         
    print('...In the last 60 minutes:')
    print(f'.....{len(cancelled_60)} cancelled jobs')
    print(f'.....{len(queued_60)} queued jobs')
    print('')
    print(f'Potential Queue Utilization Past 60 min: {len(queued_60) / QUEUE_SIZE}')
    print('')
    print('')
    print('...In the last 5 minutes:')
    print(f'.....{len(cancelled_5)} cancelled jobs')
    print(f'.....{len(queued_5)} queued jobs')
    print('')
    print(f'Potential Queue Utilization Past 5 min: {len(queued_5) / QUEUE_SIZE}')
    print('')
    print('')
    print(f"Run collection completed successfully.")

if __name__ == "__main__":
    harvest_data()