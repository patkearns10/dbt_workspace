import requests
import base64
import json
import os
from pprint import pprint

#------------------------------------------------------------------------------
# get environment variables
#------------------------------------------------------------------------------
api_base        = os.getenv('DBT_URL', 'https://cloud.getdbt.com') # default to multitenant url
job_cause       = os.getenv('DBT_JOB_CAUSE', 'API-triggered job') # default to generic message
git_branch      = os.getenv('DBT_JOB_BRANCH', None) # default to None
schema_override = os.getenv('DBT_JOB_SCHEMA_OVERRIDE', None) # default to None
api_key         = os.environ['DBT_API_KEY']  # no default here, just throw an error here if key not provided
account_id      = int(os.environ['DBT_ACCOUNT_ID']) # no default here, just throw an error here if id not provided
project_id      = int(os.environ['DBT_PROJECT_ID']) # no default here, just throw an error here if id not provided
job_id          = int(os.environ['DBT_PR_JOB_ID']) # no default here, just throw an error here if id not provided

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
run_status_map = { # dbt run statuses are encoded as integers. This map provides a human-readable status
  1:  'Queued',
  2:  'Starting',
  3:  'Running',
  10: 'Success',
  20: 'Error',
  30: 'Cancelled',
}
#------------------------------------------------------------------------------


req_auth_header = {'Authorization': f'Token {api_key}','Content-Type': 'application/json'}
repos_url = f'{api_base}/api/v3/accounts/{account_id}/projects/{project_id}/repositories/'

run_job_resp = requests.get(repos_url, headers=req_auth_header)
pprint(json.loads(run_job_resp.content))

try:
    print(run_job_resp.raise_for_status())
except Exception as e:
    print(e)

