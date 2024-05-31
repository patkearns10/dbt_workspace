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
req_projects_url = f'{api_base}/api/v3/accounts/{account_id}/users/'

# Get projects.
r = requests.get(
    url=req_projects_url,
    headers=req_auth_header,
)

payload = r.json()["data"]


# pprint(payload)

for user in payload:
    pprint("=========================================")
    pprint(user['fullname'])
    pprint("---------------------")
    pprint(f"__ id:                                 {user['id']}")
    pprint(f"__ is_active:                          {user['is_active']}")
    pprint(f"__ last_login:                         {user['last_login']}")
    pprint(f"__ apitoken_last_used:                 {user['apitoken_last_used']}")
    pprint(f"__ created_at:                         {user['created_at']}")
    pprint(f"__ email:                              {user['email']}")
    pprint(f"__ email_connected:                    {user['email_connected']}")
    pprint(f"__ email_verified:                     {user['email_verified']}")
    # pprint(f"__ auth_provider_infos:                {user['auth_provider_infos']}")
    # pprint(f"__ avatar_url:                         {user['avatar_url']}")
    # pprint(f"__ azure_active_directory_connected:   {user['azure_active_directory_connected']}")
    # pprint(f"__ azure_active_directory_username:    {user['azure_active_directory_username']}")
    # pprint(f"__ enterprise_authentication_method:   {user['enterprise_authentication_method']}")
    # pprint(f"__ enterprise_connected:               {user['enterprise_connected']}")
    # pprint(f"__ github_connected:                   {user['github_connected']}")
    # pprint(f"__ github_username:                    {user['github_username']}")
    # pprint(f"__ gitlab_connected:                   {user['gitlab_connected']}")
    # pprint(f"__ gitlab_username:                    {user['gitlab_username']}")
    # pprint(f"__ slack_connected:                    {user['slack_connected']}")

# there is also licenses and permissions. 
# I didn't bring in because you can view that in the IDE and it is very verbose.
