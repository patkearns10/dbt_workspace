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
    pprint(resp)
    pprint(resp.json()["data"])
    return resp.json()["data"]


if __name__ == "__main__":
    run = get_run()
    while run["status"] not in [
        DbtJobRunStatus.SUCCESS,
        DbtJobRunStatus.ERROR,
        DbtJobRunStatus.CANCELLED,
    ]:
        pprint(f"Run {RUN_ID} in progress.")
        try:
            # Get the max index of run_steps so we can get the step_id.
            latest_step = max(run["run_steps"], key=lambda _: _["index"])
            step_id = latest_step["id"]
            
            pprint('==================================================================')
            pprint(step_id)
            pprint('==================================================================')
            pprint('=========')
            url = f"{BASE_URL}/accounts/{ACCOUNT_ID}/steps/{step_id}/?include_related=['debug_logs']"
            resp = requests.get(url, headers=headers)
            if LOG_TYPE == "-l":
                pprint(resp.json()["data"]["logs"])
            elif LOG_TYPE == "-d":
                pprint(resp.json()["data"]["debug_logs"])
        except:
            pass
        time.sleep(1)
        run = get_run()
    pprint(f"Run {RUN_ID} completed successfully.")