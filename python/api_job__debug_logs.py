"""Live tail of a dbt run.
python live_tail.py 12345 -l
python live_tail.py 12345 -d
12345: The id of the run.
-l: Logs.
-d: Debug logs (more detail).
"""
import time
import enum
import os
import requests
import sys
from pprint import pprint

class DbtJobRunStatus(enum.IntEnum):
    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    ERROR = 20
    CANCELLED = 30


arg_list = sys.argv[1:]

DBT_API_KEY = os.environ['DBT_API_KEY']
BASE_URL = "https://cloud.getdbt.com/api/v2"
ACCOUNT_ID = 51798
RUN_ID = arg_list[0]  # 12345
LOG_TYPE = arg_list[1]  # -l / -d

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Token {DBT_API_KEY}",
}

def get_run():
    """Get the status of the run to see if it's still in progress"""
    url = (
        f"{BASE_URL}/accounts/{ACCOUNT_ID}/runs/{RUN_ID}/?include_related=['run_steps']"
    )
    resp = requests.get(url, headers=headers)
    # pprint([f'{key}: {value}' for key,value in resp.json()["data"].items() if key in ('job', 'environment', 'run_steps', 'status_humanized')])
    pprint(resp.json()["data"]["run_steps"])
    print('======')
    pprint(url)


if __name__ == "__main__":
    run = get_run()
  
