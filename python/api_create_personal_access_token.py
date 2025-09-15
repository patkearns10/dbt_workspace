import requests
import os

#------------------------------------------------------------------------------
# get environment variables
#------------------------------------------------------------------------------
api_base        = os.getenv('DBT_URL', 'https://cloud.getdbt.com') # default to multitenant url
api_key         = os.environ['DBT_API_KEY']  # no default here, just throw an error here if key not provided
account_id      = int(os.environ['DBT_ACCOUNT_ID']) # no default here, just throw an error here if id not provided
user_id         = os.environ['DBT_USER_ID'] # no default here, just throw an error here if id not provided

print(f"""
Configuration:
api_base: {api_base}
account_id: {account_id}
user_id: {user_id}
"""
)
url = f'{api_base}/api/v3/accounts/{account_id}/users/{user_id}/account-apikeys/'

payload = { "name": "API Created PAT" }
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {api_key}"
}

response = requests.post(url, json=payload, headers=headers)

print(response.json())