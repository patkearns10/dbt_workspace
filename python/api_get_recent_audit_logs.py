"""
docs: https://docs.getdbt.com/dbt-cloud/api-v3#/operations/List%20Recent%20Audit%20Log%20Events
"""

import requests
import base64
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

req_auth_header = {'Authorization': f'Bearer {api_key}','Accept': 'application/json'}
url = f'{api_base}/api/v3/accounts/{account_id}/audit-logs/?logged_at_start=2024-04-10 13:29:48.641171&logged_at_end=2030-01-01'
print(url)

run_job_resp = requests.get(url, headers=req_auth_header)

try:
    # for count, value in enumerate(json.loads(run_job_resp.content['data'])):
    #     print('======================')
    #     print(count)
    #     print('------------')
    #     print(value)
        
    pprint(json.loads(run_job_resp.content))
    # for key in json.loads(run_job_resp.content):
    #     if key in ['data']:
    #         pprint(key)
    #         for count, item in enumerate(json.loads(run_job_resp.content)[key]):
    #             print('======================')
    #             pprint(count)
    #             print('------------')
    #             pprint(item)
except Exception as e:
    print(e)

