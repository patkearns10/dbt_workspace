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
account_id      = int(os.environ['DBT_ACCOUNT_ID']) # no default here, just throw an error here if id not provided
job_id          = int(os.environ['DBT_PR_JOB_ID']) # no default here, just throw an error here if id not provided


pprint(f"""
Configuration:
api_base: {api_base}
account_id: {account_id}
job_id: {job_id}
"""
)
pprint('=======================')

#------------------------------------------------------------------------------

req_auth_header = {'Authorization': f'Token {api_key}','Content-Type': 'application/json', "Accept": "application/json",}
req_job_url = f'{api_base}/api/v2/accounts/{account_id}/jobs/{job_id}/'

r = requests.get(
    url=req_job_url,
    headers=req_auth_header,
)

# Get job configuration.
payload = r.json()["data"]
pprint('Existing data payload')
pprint('=======================')
pprint(payload)

# Is job generating docs
is_generating_docs = payload["generate_docs"]

# If not, set docs to true.
if not is_generating_docs:
    payload["generate_docs"] = True

r = requests.post(
    url=req_job_url,
    headers=req_auth_header,
    json=payload,
)

try:
    pprint('=======================')
    pprint('=======================')
    pprint(f"updating job id: {job_id}")
    pprint('=======================')
    pprint("Updated data payload:")  
    pprint(payload)
    pprint('=======================')
    pprint("Request Content:")
    pprint(json.loads(r.content))
    pprint('=======================')
    pprint('Done!')
except Exception as e:
    pprint(e)
