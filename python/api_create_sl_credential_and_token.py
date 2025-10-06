"""
When we create new SL creds + token the calls we do are
create new SL creds
create new token
create new mapping of SL creds <> token <> project
"""

import requests
import base64
import json
import os
from pprint import pprint

#------------------------------------------------------------------------------
# get environment variables
#------------------------------------------------------------------------------
api_key         = os.environ['DBT_API_KEY']  # no default here, just throw an error here if key not provided
account_id      = int(os.environ['DBT_ACCOUNT_ID']) # no default here, just throw an error here if id not provided
project_id      = int(os.environ['DBT_PROJECT_ID']) # no default here, just throw an error here if id not provided
api_base        = os.getenv('DBT_URL', 'https://cloud.getdbt.com') # default to multitenant url

print(f"""
Configuration:
account_id: {account_id}
project_id: {project_id}
api_base: {api_base}
"""
)
#------------------------------------------------------------------------------


# create SL cred
# https://emea.dbt.com/api/v3/accounts/31/semantic-layer-credentials/
# {
#   "id": null,
#   "account_id": 31,
#   "adapter_version": "snowflake_v0",
#   "schema_type": "semantic_layer_credentials",
#   "values": {
#     "auth_type": "password",
#     "role": "role",
#     "user": "usename",
#     "password": "password",
#     "warehouse": "warehouse"
#   },
#   "state": 1,
#   "name": "test-sl-token",
#   "project_id": 71
# }


# req_auth_header = {'Authorization': f'Token {api_key}','Content-Type': 'application/json', "Accept": "application/json",}
# req_job_url = f'{api_base}/api/v3/accounts/{account_id}/semantic-layer-credentials/'
# data = {
#   "id": None,
#   "account_id": account_id,
#   "adapter_version": "snowflake_v0",
#   "schema_type": "semantic_layer_credentials",
#   "values": {
#     "auth_type": "password",
#     "role": "role",
#     "user": "test_pkearns",
#     "password": "password",
#     "warehouse": "warehouse"
#   },
#   "state": 1,
#   "name": "test-sl-token-admin-password",
#   "project_id": project_id
# }

# req_payload =json.dumps(data)
# run_job_resp = requests.post(req_job_url, data=req_payload, headers=req_auth_header)
# pprint(json.loads(run_job_resp.content))

# try:
#     print(run_job_resp.raise_for_status())
# except Exception as e:
#     print(e)

# OUTPUT
# {'data': {'account_id': 51798,
#           'adapter_version': 'snowflake_v0',
#           'created_at': '2025-10-06 05:52:58.872771+00:00',
#           'id': 2092,
#           'name': 'test-sl-token',
#           'project_id': 89074,
#           'schema_type': 'semantic_layer_credentials',
#           'service_tokens': None,
#           'state': 1,
#           'updated_at': '2025-10-06 05:52:58.872781+00:00',
#           'values': {'auth_type': 'password',
#                      'role': 'role',
#                      'user': 'test_pkearns',
#                      'warehouse': 'warehouse'}},




# # create service token
# https://emea.dbt.com/api/v3/accounts/31/service-tokens/
# {
#   "id": null,
#   "state": 1,
#   "account_id": 31,
#   "name": "test-sl-token",
#   "permission_grants": [
#     {
#       "project_id": 71,
#       "permission_set": "semantic_layer_only"
#     },
#     {
#       "project_id": 71,
#       "permission_set": "metadata_only"
#     }
#   ]
# }



# req_auth_header = {'Authorization': f'Token {api_key}','Content-Type': 'application/json', "Accept": "application/json",}
# req_job_url = f'{api_base}/api/v3/accounts/{account_id}/service-tokens/'
# data = {
#   "id": None,
#   "state": 1,
#   "account_id": account_id,
#   "name": "test-sl-token-3",
#   "permission_grants": [
#     {
#       "project_id": project_id,
#       "permission_set": "semantic_layer_only"
#     },
#     {
#       "project_id": project_id,
#       "permission_set": "metadata_only"
#     }
#   ]
# }

# req_payload =json.dumps(data)
# run_job_resp = requests.post(req_job_url, data=req_payload, headers=req_auth_header)
# pprint(json.loads(run_job_resp.content))

# try:
#     print(run_job_resp.raise_for_status())
# except Exception as e:
#     print(e)

# OUTPUT:
# {'data': {'account_id': 51798,
#           'created_at': '2025-10-06 05:55:54.587854+00:00',
#           'expires_at': None,
#           'id': 72512,
#           'last_identify': None,
#           'last_used': None,
#           'metadata_only': False,
#           'name': 'test-sl-token-2',
#           'permission_grants': [{'permission_set': 'semantic_layer_only',
#                                  'project_id': 89074,
#                                  'writable_environment_categories': []},
#                                 {'permission_set': 'metadata_only',
#                                  'project_id': 89074,
#                                  'writable_environment_categories': []}],
#           'semantic_layer_credentials': None,
#           'service_token_permissions': [{'account_id': 51798,
#                                          'all_projects': False,
#                                          'created_at': '2025-10-06 '
#                                                        '05:55:54.651376+00:00',
#                                          'id': 116639,
#                                          'permission_set': 'semantic_layer_only',
#                                          'project_id': 89074,
#                                          'service_token_id': 72512,
#                                          'state': 1,
#                                          'updated_at': '2025-10-06 '
#                                                        '05:55:54.651386+00:00',
#                                          'writable_environment_categories': []},
#                                         {'account_id': 51798,
#                                          'all_projects': False,
#                                          'created_at': '2025-10-06 '
#                                                        '05:55:54.682007+00:00',
#                                          'id': 116640,
#                                          'permission_set': 'metadata_only',
#                                          'project_id': 89074,
#                                          'service_token_id': 72512,
#                                          'state': 1,
#                                          'updated_at': '2025-10-06 '
#                                                        '05:55:54.682015+00:00',
#                                          'writable_environment_categories': []}],
#           'state': 1,
#           'token': '1E4Bc1DYaA-UNJlSdzOAyFL_Ll0eoGXxx1yqLFv_t0M',
#           'token_string': 'dbtc_iAQvMfw1E4Bc1DYaA-UNJlSdzOAyFL_Ll0eoGXxx1yqLFv_t0M',
#           'type': None,
#           'uid': 'iAQvMfw',
#           'updated_at': '2025-10-06 05:55:54.587865+00:00',
#           'user': None,
#           'user_id': None,
#           'webhooks_only': False},

# # assign SL creds to token and to project
# https://emea.dbt.com/api/v3/accounts/31/semantic-layer-credential-to-service-token-mapping/
# {
#   "account_id": 31,
#   "project_id": 71,
#   "semantic_layer_credentials_id": 403,
#   "service_token_id": 2635962,
#   "state": 1
# }



req_auth_header = {'Authorization': f'Token {api_key}','Content-Type': 'application/json', "Accept": "application/json",}
req_job_url = f'{api_base}/api/v3/accounts/{account_id}/semantic-layer-credential-to-service-token-mapping/'
data = {
  "account_id": account_id,
  "project_id": project_id,
  "semantic_layer_credentials_id": 2093,
  "service_token_id": 72514,
  "state": 1
}

req_payload =json.dumps(data)
run_job_resp = requests.post(req_job_url, data=req_payload, headers=req_auth_header)
pprint(json.loads(run_job_resp.content))

try:
    print(run_job_resp.raise_for_status())
except Exception as e:
    print(e)

    # OUTPUT:

    # {'data': {'account_id': 51798,
    #       'created_at': '2025-10-06 05:59:55.055496+00:00',
    #       'id': 3264,
    #       'project_id': 89074,
    #       'semantic_layer_credentials': {'account_id': 51798,
    #                                      'adapter_version': 'snowflake_v0',
    #                                      'created_at': '2025-10-06 '
    #                                                    '05:52:58.872771+00:00',
    #                                      'id': 2092,
    #                                      'name': 'test-sl-token',
    #                                      'project_id': 89074,
    #                                      'schema_type': 'semantic_layer_credentials',
    #                                      'service_tokens': None,
    #                                      'state': 1,
    #                                      'updated_at': '2025-10-06 '
    #                                                    '05:52:58.872781+00:00',
    #                                      'values': {'auth_type': 'password',
    #                                                 'role': 'role',
    #                                                 'user': 'test_pkearns',
    #                                                 'warehouse': 'warehouse'}},
    #       'semantic_layer_credentials_id': 2092,
    #       'service_token': {'account_id': 51798,
    #                         'created_at': '2025-10-06 05:55:54.587854+00:00',
    #                         'expires_at': None,
    #                         'id': 72512,
    #                         'last_identify': None,
    #                         'last_used': None,
    #                         'metadata_only': False,
    #                         'name': 'test-sl-token-2',
    #                         'semantic_layer_credentials': None,
    #                         'service_token_permissions': [],
    #                         'state': 1,
    #                         'token': None,
    #                         'token_string': None,
    #                         'type': None,
    #                         'uid': 'iAQvMfw',
    #                         'updated_at': '2025-10-06 05:55:54.587865+00:00',
    #                         'user': None,
    #                         'user_id': None,
    #                         'webhooks_only': False},
    #       'service_token_id': 72512,
    #       'state': 1,
    #       'updated_at': '2025-10-06 05:59:55.055507+00:00'},