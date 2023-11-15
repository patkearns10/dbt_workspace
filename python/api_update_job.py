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
environment_id  = int(os.environ['DBT_ENVIRONMENT_ID']) # no default here, just throw an error here if id not provided

print(f"""
Configuration:
api_base: {api_base}
job_cause: {job_cause}
git_branch: {git_branch}
schema_override: {schema_override}
account_id: {account_id}
project_id: {project_id}
job_id: {job_id}
environment_id: {environment_id}
"""
)
#------------------------------------------------------------------------------

req_auth_header = {'Authorization': f'Token {api_key}','Content-Type': 'application/json', "Accept": "application/json",}
req_job_url = f'{api_base}/api/v2/accounts/{account_id}/jobs/{job_id}/'
data = {
    "execution": {
        "timeout_seconds": 0
    },
    "generate_docs": True,
    "run_generate_sources": False,
    "id": job_id,
    "account_id": account_id,
    "project_id": project_id,
    "environment_id": environment_id,
    "name": "dbt run",
    "dbt_version": null,
    "state": 1,
    "triggers": {
        "github_webhook": False,
        "git_provider_webhook": False,
        "custom_branch_only": False,
        "schedule": False
    },
    "settings": {
        "threads": 4,
        "target_name": "prod"
    },
    "schedule": {
        "cron": "0 * * * 0,1,2,3,4,5,6",
        "date": {
            "type": "days_of_week",
            "days": [
                0,
                1,
                2,
                3,
                4,
                5,
                6
            ]
        },
        "time": {
            "type": "every_hour",
            "interval": 1
        }
    },
}
 

req_payload =json.dumps(data)
run_job_resp = requests.post(req_job_url, data=req_payload, headers=req_auth_header)
pprint(json.loads(run_job_resp.content))

try:
    print(run_job_resp.raise_for_status())
except Exception as e:
    print(e)
