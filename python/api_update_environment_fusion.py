import requests
import os
from pprint import pprint


#------------------------------------------------------------------------------
# get environment variables
#------------------------------------------------------------------------------
api_base        = os.getenv('DBT_HOST', 'https://cloud.getdbt.com') # default to multitenant url
api_key         = os.environ['DBT_API_KEY']  # no default here, just throw an error here if key not provided
account_id      = int(os.environ['DBT_ACCOUNT_ID']) # no default here, just throw an error here if id not provided
environment_id  = int(os.environ['DBT_ENVIRONMENT_ID']) # no default here, just throw an error here if id not provided

pprint('Configurations:')
pprint('=======================')
pprint(f"api_base: {api_base}")
pprint(f"account_id: {account_id}")
pprint(f"environment_id: {environment_id}")
pprint('=======================')

pprint(f"Environment Summary")
pprint('=======================')

def print_env_summary(data):
    pprint({
        'project_id': data.get('project_id'),
        'environment_id': data.get('id'),
        'environment_name': data.get('name'),
        'deployment_type': data.get('deployment_type'),
        'dbt_version': data.get('dbt_version'),
        'raw_dbt_version': data.get('raw_dbt_version'),
    })


#------------------------------------------------------------------------------

req_auth_header = {'Authorization': f'Token {api_key}', 'Content-Type': 'application/json', 'Accept': 'application/json'}
req_env_url = f'{api_base}/api/v2/accounts/{account_id}/environments/{environment_id}/'

r = requests.get(
    url=req_env_url,
    headers=req_auth_header,
)

# Get environment configuration.
payload = r.json()["data"]

print_env_summary(payload)

# Is environment already on fusion-nightly?
current_version = payload.get("dbt_version")

if current_version == "fusion-nightly":
    pprint('=======================')
    pprint(f"Environment Not Updated")
    pprint('=======================')
    pprint(f"Environment {environment_id} is already on fusion-nightly. No change made.")
else:
    payload["dbt_version"] = "fusion-nightly"
    payload["raw_dbt_version"] = "fusion-nightly"

    r = requests.post(
        url=req_env_url,
        headers=req_auth_header,
        json=payload,
    )

    try:
        pprint('=======================')
        pprint(f"updating environment id: {environment_id}")
        pprint('=======================')
        print_env_summary(payload)
        pprint('Done!')
    except Exception as e:
        pprint(e)