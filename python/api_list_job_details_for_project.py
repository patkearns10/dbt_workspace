"""
Export your variables:
----------------------
export DBT_ACCOUNT_ID=51798
export DBT_PROJECT_ID=89074
export DBT_API_KEY='dbtc_aaaaabbbbbbccccddddeeeeefffff'

Run your script:
----------------------
python3 api_list_job_details_for_project.py
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
job_cause       = os.getenv('DBT_JOB_CAUSE', 'API-triggered job') # default to generic message

schema_override = os.getenv('DBT_JOB_SCHEMA_OVERRIDE', None) # default to None
api_key         = os.environ['DBT_API_KEY']  # no default here, just throw an error here if key not provided
account_id      = int(os.environ['DBT_ACCOUNT_ID']) # no default here, just throw an error here if id not provided
project_id      = int(os.environ['DBT_PROJECT_ID']) # no default here, just throw an error here if id not provided


pprint('=======================')
pprint('Configuration:')
pprint('-----------------------')
pprint(f'api_base: {api_base}')
pprint(f'account_id: {account_id}')
pprint(f'project_id: {project_id}')
pprint('=======================')

req_auth_header = {'Authorization': f'Token {api_key}','Content-Type': 'application/json', "Accept": "application/json",}

#------------------------------------------------------------------------------
# get environments

def get_environments():
    req_environments_url = f'{api_base}/api/v3/accounts/{account_id}/projects/{project_id}/environments/'

    r = requests.get(
        url=req_environments_url,
        headers=req_auth_header,
    )

    return r.json()["data"]

#------------------------------------------------------------------------------
# get all jobs

def get_all_jobs():
    r = requests.get(
        url=f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/",
        headers=req_auth_header,
    )
    return r.json()["data"]
    
#------------------------------------------------------------------------------
# get jobs per environment

def get_jobs_per_environment():

    jobs_data = get_all_jobs()
    environment_jobs = []

    for job in jobs_data:
        if job["state"] == 1 and job["deactivated"] == False:
            jobs_dict = {
                'environment_id': job["environment_id"],
                'job_name':       job["name"],
                'job_id':         job["id"],
                'target_name':    job["settings"]["target_name"],
                'dbt_version':    job["dbt_version"],
                'job_type':       job["job_type"],
                'description':    job["description"],
                'execute_steps':  job["execute_steps"],
            }
            environment_jobs.append(jobs_dict)

    return environment_jobs

#------------------------------------------------------------------------------
# print stuff

environments = get_environments()
jobs = get_jobs_per_environment()

print(f'')
pprint('#######################')
pprint('Environments:')
pprint('#######################')
print(f'')

for item in environments:
    if item["type"] != 'development':
        print(f'')
        pprint('    =======================')
        print(f'     environment_name: {item["name"]}')
        print(f'     environment_id:   {item["id"]}')
        pprint('    =======================')
        print(f'')
        print(f'     dbt_version:      {item["dbt_version"]}')
        print(f'     type:             {item["type"]}')
        print(f'     database:         {item["credentials"]["database"]}')
        print(f'     role:             {item["credentials"]["role"]}')
        print(f'     schema:           {item["credentials"]["schema"]}')
        print(f'     target_name:      {item["credentials"]["target_name"]}')
        print(f'     user:             {item["credentials"]["user"]}')
        print(f'     warehouse:        {item["credentials"]["warehouse"]}')
        print(f'')
        pprint('      ----------------------------')
        pprint('      Jobs:                      ')
        pprint('      ----------------------------')
        print(f'')

        for job in jobs:
            if item["id"] == job["environment_id"]:
                print(f'')
                pprint('        ........................')
                print(f'         job_name:       {job["job_name"]}')
                pprint('        ........................')
                print(f'')
                print(f'          job_id:         {job["job_id"]}')
                print(f'          target_name:    {job["target_name"]}')
                print(f'          dbt_version:    {job["dbt_version"]}')
                print(f'          job_type:       {job["job_type"]}')
                print(f'          description:    {job["description"]}')
                print(f'          execute_steps:  {job["execute_steps"]}')
                print(f'')
                pprint('        ........................')
                print(f'')

print('Done!')
