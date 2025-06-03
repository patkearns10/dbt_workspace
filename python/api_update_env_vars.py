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
project_id      = int(os.environ['DBT_PROJECT_ID']) # no default here, just throw an error here if id not provided

print(f"""
Configuration:
api_base: {api_base}
account_id: {account_id}
project_id: {project_id}
"""
)
#------------------------------------------------------------------------------


#------------------------------------------------------------------------------
# use environment variables to set configuration
#------------------------------------------------------------------------------

headers = {'Authorization': f'Token {api_key}','Content-Type': 'application/json', "Accept": "application/json"}
url = f'{api_base}/api/v3/accounts/{account_id}/projects/{project_id}/environment-variables/bulk/'
payload = {
	"env_vars": {
		1273348: "smallish",
        1273349: "bigish",
        1273347: "gigantic",
		"name": "DBT_WAREHOUSE_SIZE",

	}
}

response = requests.put(url, json=payload, headers=headers)
print(response.json())

