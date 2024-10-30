"""
https://au.dbt.com/api/v2/accounts/{account_id}/licenses/
https://au.dbt.com/api/v2/accounts/{account_id}/users/
"""

import base64
import csv
from datetime import datetime
import json
import requests
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

# extract the current filepath
DIR_PATH = os.path.dirname(os.path.abspath(__file__))
now = datetime.now().strftime('%Y.%m.%d.%H.%M.%S')
req_auth_header = {'Authorization': f'Bearer {api_key}','Accept': 'application/json'}

# licenses csv
license_fields = ['license', 'available', 'assigned', 'in_use', 'over_limit']
def create_licenses_csv_dict(license, available, assigned, in_use, over_limit):
    return {
    "license": f"{license}",
    "available": f"{available}",
    "assigned": f"{assigned}",
    "in_use": f"{in_use}",
    "over_limit": f"{over_limit}"
    }

def get_licenses(req_auth_header=req_auth_header, api_key=api_key, account_id=account_id):
    url = f'{api_base}/api/v2/accounts/{account_id}/licenses/'
    license_resp = requests.get(url, headers=req_auth_header)
    json_payload = json.loads(license_resp.content)
    try:
        # Opening new file and JSON file
        with open(os.path.join(DIR_PATH, f'dbt_licenses_{now}.csv'), 'w') as dbt_licenses_file:
            # creating a csv dict writer object
            writer = csv.DictWriter(dbt_licenses_file, fieldnames=license_fields)
            writer.writeheader()
            for key, value in json_payload['data'].items():
                print('======================')
                pprint(key.upper())
                print('---------')
                pprint(f"available:  {value['available']}")
                pprint(f"assigned:   {value['assigned']}")
                pprint(f"in_use:     {value['in_use']}")
                pprint(f"over_limit: {value['over_limit']}")
                print('======================')

                writer.writerow(
                    create_licenses_csv_dict(
                        key.upper(),
                        value['available'],
                        value['assigned'],
                        value['in_use'],
                        value['over_limit'],
                        )
                    )
            print('Succesfully exported licenses file!')
    except Exception as e:
        print(e)

# users csv
users_fields = ['id', 'first_name', 'last_name', 'email', 'apitoken_last_used', 'is_active', 'last_login', 'created_at', 'license_type']
def create_users_csv_dict(id, first_name, last_name, email, apitoken_last_used, is_active, last_login, created_at, license_type):
    return {
    "id": f"{id}",
    "first_name": f"{first_name}",
    "last_name": f"{last_name}",
    "email": f"{email}",
    "apitoken_last_used": f"{apitoken_last_used}",
    "is_active": f"{is_active}",
    "last_login": f"{last_login}",
    "created_at": f"{created_at}",
    "license_type": f"{license_type}",
    }

def get_users(req_auth_header=req_auth_header, api_key=api_key, account_id=account_id):
    url = f'{api_base}/api/v2/accounts/{account_id}/users/'
    users_resp = requests.get(url, headers=req_auth_header)
    json_payload = json.loads(users_resp.content)
    try:
        # Opening new file and JSON file
        with open(os.path.join(DIR_PATH, f'dbt_users_{now}.csv'), 'w') as dbt_users_file:
            # creating a csv dict writer object
            writer = csv.DictWriter(dbt_users_file, fieldnames=users_fields)
            writer.writeheader()
            for user in json_payload['data']:
                writer.writerow(
                    create_users_csv_dict(
                        user['id'],
                        user['first_name'],
                        user['last_name'],
                        user['email'],
                        user['apitoken_last_used'],
                        user['is_active'],
                        user['last_login'],
                        user['created_at'],
                        user['permissions'][0]['license_type'],
                        )
                    )
            print('Succesfully exported users file!')
    except Exception as e:
        print(e)

if __name__ == "__main__":
    get_licenses()
    get_users()
