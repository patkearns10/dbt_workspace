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

def get_watermark():
    # ping s3 api to return last updated timestamp
    # if no files in the s3 bucket, then return a sane timestamp, like:
    last_run_max_timestamp = '2020-04-10 13:29:48.641171'
    # optionally add an argument to this function to provide a timestamp instead
    pprint(f'last_run_max_timestamp: {last_run_max_timestamp}')
    return last_run_max_timestamp

log_count = []

def get_audit_logs(api_key=api_key, account_id=account_id, logged_at_start='1901-01-01', logged_at_end='2030-01-01'):
    req_auth_header = {'Authorization': f'Bearer {api_key}','Accept': 'application/json'}
    repos_url = f'{api_base}/api/v3/accounts/{account_id}/audit-logs/?logged_at_start={logged_at_start}&logged_at_end={logged_at_end}&order_by=logged_at'

    run_job_resp = requests.get(repos_url, headers=req_auth_header)

    audit_logs = []
    pprint(json.loads(run_job_resp.content))
    
    try:
        pprint(run_job_resp.raise_for_status())
        json_payload = json.loads(run_job_resp.content)
        for key in json_payload:
            if key in {'extra'}:
                # log this output to stdout
                print('======================')
                pprint(json_payload[key])
                print('======================')
                log_count.append(json_payload[key]['pagination'])
            if key in ['data']:
                # pprint(json_payload[key])
                # collect this info in a list of dicts to update a table or add json to a file.
                [audit_logs.append(dict) for dict in json_payload[key]]
        pprint(audit_logs)
        return audit_logs
    except Exception as e:
        print(e)

def upload_to_s3(audit_logs):
    #code to add file to s3
    # do something with audit_logs
    pprint("Successfully added file to s3")

if __name__ == "__main__":
    last_run_max_timestamp = get_watermark()
    audit_logs = get_audit_logs(logged_at_start=last_run_max_timestamp)
    print('======================')
    upload_to_s3(audit_logs)
    # if more than 50 here, increase limit to 100
    # if more than 100, then add pagination
    pprint(f'Uploaded: {log_count[0]["count"]} of {log_count[0]["total_count"]}')
