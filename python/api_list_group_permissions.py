import requests
import json
import os
from pprint import pprint

#------------------------------------------------------------------------------
# get environment variables
#------------------------------------------------------------------------------
api_base        = os.getenv('DBT_URL', 'https://cloud.getdbt.com') # default to multitenant url
api_key         = os.environ['DBT_API_KEY']  # no default here, just throw an error here if key not provided
account_id      = int(os.environ['DBT_ACCOUNT_ID']) # no default here, just throw an error here if id not provided

print(f"""
Configuration:
api_base: {api_base}
account_id: {account_id}
"""
)
#------------------------------------------------------------------------------

req_auth_header = {'Authorization': f'Token {api_key}','Content-Type': 'application/json'}
repos_url = f'{api_base}/api/v3/accounts/{account_id}/groups/'
run_job_resp = requests.get(repos_url, headers=req_auth_header)

try:
    print(run_job_resp.raise_for_status())
except Exception as e:
    pprint(e)

# pprint(json.loads(run_job_resp.content))

for group in json.loads(run_job_resp.content)['data']:
    if group["state"] == 1:
        print(f'')
        pprint('    =======================')
        print(f'     name:                  {group["name"]}')
        print(f'     sso_mapping_groups:    {group["sso_mapping_groups"]}')
        print(f'')
        for permission in group["group_permissions"]:
            pprint('    ----group_permissions--------')
            if permission["state"] == 1:
                print(f'')
                print(f'        permission_set:         {permission["permission_set"]}')
                print(f'            all_projects:       {permission["all_projects"]}')
                print(f'            project_id:         {permission["project_id"]}')

