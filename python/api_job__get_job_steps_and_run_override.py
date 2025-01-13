from datetime import datetime
import requests
import json
import os

from pprint import pprint

#------------------------------------------------------------------------------
# get environment variables
#------------------------------------------------------------------------------
api_base        = os.getenv('DBT_URL', 'https://cloud.getdbt.com') # default to multitenant url
job_cause       = os.getenv('DBT_JOB_CAUSE', 'API-triggered job') # default to generic message
api_key         = os.environ['DBT_API_KEY']  # no default here, just throw an error here if key not provided
account_id      = os.environ['DBT_ACCOUNT_ID'] # no default here, just throw an error here if id not provided
job_id          = os.environ['DBT_PR_JOB_ID'] # no default here, just throw an error here if id not provided

# THIS SIMULATES YOUR METHOD OF BATCHING BY DATE
audit_date      = datetime.today().strftime('%Y-%m-%d')
#------------------------------------------------------------------------------

print(f"""
Configuration:
api_base: {api_base}
job_cause: {job_cause}
account_id: {account_id}
job_id: {job_id}
"""
)

#------------------------------------------------------------------------------
# use environment variables to set configuration
#------------------------------------------------------------------------------
req_auth_header = {'Authorization': f'Token {api_key}','Content-Type': 'application/json', "Accept": "application/json",}
req_job_url = f'{api_base}/api/v2/accounts/{account_id}/jobs/{job_id}/'
#------------------------------------------------------------------------------

# GET JOB COMMAND FROM WHATEVER IS WITHIN DBT JOB COMMANDS
def get_job_command_steps(url, headers):
    r = requests.get(
        url=req_job_url,
        headers=req_auth_header,
    )

    # Get job current configuration.
    execute_steps_commands = r.json()["data"]['execute_steps']
    return execute_steps_commands


def run_job(url, headers, cause, audit_date, execute_steps_commands) -> int:
    """
    Runs a dbt job using settings from dbt, with steps override
    """

    # TAKE THE COMMANDS FROM WITHIN DBT AND ADD THE DATE VARIABLE TO THE COMMAND
    steps_override  = f" --vars '{{\"audit_date\": \"{audit_date}\"}}'"
    execute_steps = [step + steps_override for step in execute_steps_commands]

    # build payload
    req_payload = {
        "cause": "API triggered",
        "steps_override": execute_steps
    }

    # trigger job, add 'run' to the URL from before
    req_payload = json.dumps(req_payload)
    run_job_resp = requests.post(url+'run', headers=headers, data=req_payload).json()
    pprint(f'Kicked off job: {job_id} with steps override!')


if __name__ == "__main__":
    execute_steps_commands = get_job_command_steps(req_auth_header, req_job_url)
    run_job(req_job_url, req_auth_header, job_cause, audit_date, execute_steps_commands)

