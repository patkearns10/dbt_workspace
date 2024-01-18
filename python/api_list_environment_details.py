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



pprint('=======================')
pprint('Configuration:')
pprint('-----------------------')
pprint(f'api_base: {api_base}')
pprint(f'account_id: {account_id}')
pprint('=======================')

#------------------------------------------------------------------------------
req_auth_header = {'Authorization': f'Token {api_key}','Content-Type': 'application/json', "Accept": "application/json",}
req_projects_url = f'{api_base}/api/v3/accounts/{account_id}/projects/?include_related=pk'

# Get projects.
r = requests.get(
    url=req_projects_url,
    headers=req_auth_header,
)

payload = r.json()["data"]

projects = []
for item in payload:
    projects.append(item['id'])

for project_id in projects:
    pprint('=======================')
    pprint(f'project_id: {project_id}')
    pprint('=======================')
    req_environments_url = f'{api_base}/api/v3/accounts/{account_id}/projects/{project_id}/environments/'

    r = requests.get(
        url=req_environments_url,
        headers=req_auth_header,
    )

    # Get environments data
    payload = r.json()["data"]
    pprint('    Environments:')
    pprint('    =======================')
    # pprint(payload)

    environments = {}
    for item in payload:
        print(f'    environment_id: {item["id"]}')
        print(f'    environment_name: {item["name"]}')
        print(f'    dbt_version: {item["dbt_version"]}')
        print(f'    raw_dbt_version: {item["raw_dbt_version"]}')
        print(f'    state: {item["state"]}')
        print(f'    type: {item["type"]}')
        pprint('    =======================')
    