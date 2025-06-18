import requests
import json
import os
import time

#------------------------------------------------------------------------------
# get environment variables
#------------------------------------------------------------------------------
api_base        = os.getenv('DBT_URL', 'https://cloud.getdbt.com') # default to multitenant url
job_cause       = os.getenv('DBT_JOB_CAUSE', 'API-triggered job') # default to generic message
git_branch      = os.getenv('DBT_JOB_BRANCH', None) # default to None
schema_override = os.getenv('DBT_JOB_SCHEMA_OVERRIDE', None) # default to None
pr_id           = os.getenv('DBT_PR_ID', None) # default to None
# steps           = os.getenv('DBT_STEPS_OVERRIDE', None) # default to None
api_key         = os.environ['DBT_API_KEY']  # no default here, just throw an error here if key not provided
account_id      = os.environ['DBT_ACCOUNT_ID'] # no default here, just throw an error here if id not provided
project_id      = os.environ['DBT_PROJECT_ID'] # no default here, just throw an error here if id not provided
job_id          = os.environ['DBT_PR_JOB_ID'] # no default here, just throw an error here if id not provided
is_slim_ci_job = False


print(f"""
Configuration:
api_base: {api_base}
job_cause: {job_cause}
git_branch: {git_branch}
schema_override: {schema_override}
account_id: {account_id}
project_id: {project_id}
job_id: {job_id}
"""
)
#------------------------------------------------------------------------------


#------------------------------------------------------------------------------
# use environment variables to set configuration
#------------------------------------------------------------------------------
req_auth_header = {'Authorization': f'Token {api_key}','Content-Type': 'application/json'}
req_job_url = f'{api_base}/api/v2/accounts/{account_id}/jobs/{job_id}/run/'
run_status_map = { # dbt run statuses are encoded as integers. This map provides a human-readable status
  1:  'Queued',
  2:  'Starting',
  3:  'Running',
  10: 'Success',
  20: 'Error',
  30: 'Cancelled',
}
#------------------------------------------------------------------------------

def run_job(url, headers, cause, branch=None, schema=None) -> int:
  """
  Runs a dbt job
  """
  # build payload
  req_payload = {
    "cause": "API triggered",
    "steps_override": [
        "dbt run -s incremental_steps_override --vars '{is_backfill: true, refresh_start_date: '2024-01-03', refresh_end_date: '2024-01-05'}'",
        "dbt run -s incremental_steps_override --vars '{is_backfill: true, refresh_start_date: '2024-01-05', refresh_end_date: '2024-01-07'}'",
        ]
  }
  if branch and not branch.startswith('$('): # starts with '$(' indicates a valid branch name was not provided
    req_payload['git_branch'] = branch.replace('refs/heads/', '')
  if schema_override:
    req_payload['schema_override'] = schema_override.replace('-', '_')
  if is_slim_ci_job:
    req_payload['schema_override'] = f'dbt_cloud_pr_{job_id}_{pr_id}'

  # trigger job
  req_payload = json.dumps(req_payload)
  print(f'Triggering job:\n\turl: {url}\n\tpayload: {req_payload}')
  run_job_resp = requests.post(url, headers=headers, data=req_payload).json()

  # return run id
  return run_job_resp['data']['id']


def get_run_status(url, headers) -> str:
  """
  gets the status of a running dbt job
  """
  # get status
  req_status_resp = requests.get(url, headers=headers).json()

  # return status
  run_status_code = req_status_resp['data']['status']
  run_status = run_status_map[run_status_code]
  return run_status


def main():
  print('Beginning request for job run...')

  # run job
  run_id: int = None
  try:
    run_id = run_job(req_job_url, req_auth_header, job_cause, git_branch, schema_override)
  except Exception as e:
    print(f'ERROR! - Could not trigger job:\n {e}')
    raise

  # build status check url and run status link
  req_status_url = f'{api_base}/api/v2/accounts/{account_id}/runs/{run_id}/'
  run_status_link = f'{api_base}/deploy/{account_id}/projects/{project_id}/runs/{run_id}/'

  # update user with status link
  print(f'Job running! See job status at {run_status_link}')

  # check status indefinitely with an initial wait period
  time.sleep(30)
  while True:
    status = get_run_status(req_status_url, req_auth_header)
    print(f'Run status -> {status}')

    if status in ['Error', 'Cancelled']:
      raise Exception(f'Run failed or canceled. See why at {run_status_link}')

    if status == 'Success':
      print(f'Job completed successfully! See details at {run_status_link}')
      return
    
    time.sleep(10)


if __name__ == "__main__":
    main()

