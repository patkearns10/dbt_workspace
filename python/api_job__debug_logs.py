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



# debug logs need step_id
# def get_run():
#     """Get the status of the run to see if it's still in progress"""
#     url = (
#         f"{BASE_URL}/accounts/{ACCOUNT_ID}/steps/{STEP_ID}/?include_related=['debug_logs']"
#     )
#     resp = requests.get(url, headers=headers)
#     pprint(resp.json()["data"])

# def get_run():
#     """Get the status of the run to see if it's still in progress"""
#     url = (
#         f"{BASE_URL}/accounts/{ACCOUNT_ID}/runs/{RUN_ID}/?include_related=['logs']"
#     )
#     resp = requests.get(url, headers=headers)


#     pprint(resp.json()["data"])
#     pprint('==========')
#     pprint([f'{key}: {value}' for key,value in resp.json()["data"].items() if key in ('id', 'trigger_id', 'job_definition_id', 'status', 'status_message', 'trigger', 'job', 'status_humanized', 'job_id', 'logs')])


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

# 'name': 'Invoke dbt with `dbt build`',
#                 'run_id': 230324528,
#                 'run_step_command': None,
#                 'started_at': '2023-12-18 22:20:33.787510+00:00',
#                 'status': 10,


# dict_keys(['id', 'trigger_id', 'account_id', 'environment_id', 'project_id', 'job_definition_id', 'status', 'dbt_version', 'git_branch', 'git_sha', 'status_message', 'owner_thread_id', 'executed_by_thread_id', 'deferring_run_id', 'artifacts_saved', 'artifact_s3_path', 'has_docs_generated', 'has_sources_generated', 'notifications_sent', 'blocked_by', 'scribe_enabled', 'created_at', 'updated_at', 'dequeued_at', 'started_at', 'finished_at', 'last_checked_at', 'last_heartbeat_at', 'should_start_at', 'trigger', 'job', 'environment', 'run_steps', 'status_humanized', 'in_progress', 'is_complete', 'is_success', 'is_error', 'is_cancelled', 'duration', 'queued_duration', 'run_duration', 'duration_humanized', 'queued_duration_humanized', 'run_duration_humanized', 'created_at_humanized', 'finished_at_humanized', 'retrying_run_id', 'can_retry', 'retry_not_supported_reason', 'job_id', 'is_running', 'href', 'used_repo_cache'])


if __name__ == "__main__":
    run = get_run()
    # while run["status"] not in [
    #     DbtJobRunStatus.SUCCESS,
    #     DbtJobRunStatus.ERROR,
    #     DbtJobRunStatus.CANCELLED,
    # ]:
    #     pprint(f"Run {RUN_ID} in progress.")
    #     try:
    #         # Get the max index of run_steps so we can get the step_id.
    #         latest_step = max(run["run_steps"], key=lambda _: _["index"])
    #         step_id = latest_step["id"]
    #         url = f"{BASE_URL}/accounts/{ACCOUNT_ID}/steps/{step_id}/?include_related=['debug_logs']"
    #         resp = requests.get(url, headers=headers)
    #         if LOG_TYPE == "-l":
    #             pprint(resp.json()["data"]["logs"])
    #         elif LOG_TYPE == "-d":
    #             pprint(resp.json()["data"]["debug_logs"])
    #     except:
    #         pass
    #     time.sleep(1)
    #     run = get_run()
    # pprint(f"Run {RUN_ID} completed successfully.")