import base64
import json
import os
import re
import requests

from pprint import pprint

#------------------------------------------------------------------------------
# get environment variables
#------------------------------------------------------------------------------
api_base        = os.getenv('DBT_URL', 'https://cloud.getdbt.com') # default to multitenant url
api_key         = os.environ['DBT_API_KEY']  # no default here, just throw an error here if key not provided
account_id      = int(os.environ['DBT_ACCOUNT_ID']) # no default here, just throw an error here if id not provided
log_file_name   = os.getenv('DBT_LOG_FILE_NAME', 'dbt_cloud_run') # default to dbt_cloud_logs
artifact        = os.getenv('DBT_ARTIFACT_TYPE', 'manifest.json') # default to console_logs (or change to debug_logs)


print(f"""
Configuration:
api_base: {api_base}
account_id: {account_id}
log_file_name: {log_file_name}
artifact: {artifact}
"""
)
#------------------------------------------------------------------------------
# extract the current filepath
DIR_PATH = os.path.dirname(os.path.abspath(__file__))
req_auth_header = {'Authorization': f'Token {api_key}','Content-Type': 'application/json'}

def get_latest_run_id():
    querystring = {"account_id":f'{account_id}',"order_by":"-id","limit":"1"}
    url = f'{api_base}/api/v2/accounts/{account_id}/runs/'

    response = requests.get(url, headers=req_auth_header, params=querystring)
    
    if response.status_code != 200:
        raise Exception(f"Request failed: {response.text}, API Call made to the following URI: {endpoint}")

    latest_run_id = json.loads(response.content)['data'][0]['id']
    pprint(latest_run_id)
    return latest_run_id


def get_job_artifact(latest_run_id):
    # Creating the URL endpoint
    endpoint = f'{api_base}/api/v2/accounts/{account_id}/runs/{latest_run_id}/artifacts/{artifact}'
    # Make the API Call to get the data
    response = requests.get(endpoint, headers=req_auth_header)
    
    # Check status code
    if response.status_code != 200:
        raise Exception(f"Request failed: {response.text}, API Call made to the following URI: {endpoint}")
    
    # Turn data into JSON
    data = response.json()

    # define the log file name to output to 
    formal_log_file_name = f"{latest_run_id}_{artifact}"

    # Save the concatenated logs to the specified output file
    with open(os.path.join(DIR_PATH,formal_log_file_name), 'w') as file:
            json.dump(data, file, indent=4) 
            
    
    # Logging the completion of the task
    pprint(f'Successfully saved logs to {os.path.join(DIR_PATH,formal_log_file_name)}')

if __name__ == '__main__':
    latest_run_id = get_latest_run_id()
    get_job_artifact(latest_run_id)