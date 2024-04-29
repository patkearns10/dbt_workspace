import requests
import os
from pprint import pprint
import time

#------------------------------------------------------------------------------
# get environment variables
#------------------------------------------------------------------------------
api_base        = os.getenv('DBT_URL', 'https://cloud.getdbt.com') # default to multitenant url
job_cause       = os.getenv('DBT_JOB_CAUSE', 'API-triggered job') # default to generic message
api_key         = os.environ['DBT_API_KEY']  # no default here, just throw an error here if key not provided
account_id      = os.environ['DBT_ACCOUNT_ID'] # no default here, just throw an error here if id not provided
job_id          = os.environ['DBT_PR_JOB_ID'] # no default here, just throw an error here if id not provided



print(f"""
Configuration:
api_base: {api_base}
job_cause: {job_cause}
account_id: {account_id}
job_id: {job_id}
"""
)
#------------------------------------------------------------------------------


#------------------------------------------------------------------------------
# use environment variables to set configuration
#------------------------------------------------------------------------------
req_auth_header = {'Authorization': f'Token {api_key}'}
req_job_url = f'{api_base}/api/v2/accounts/{account_id}/runs/?job_definition_id={job_id}&include_related=[%27trigger%27,%27job%27]&order_by=-id&limit=1'
run_status_map = { # dbt run statuses are encoded as integers. This map provides a human-readable status
  1:  'Queued',
  2:  'Starting',
  3:  'Running',
  10: 'Success',
  20: 'Error',
  30: 'Cancelled',
}
#------------------------------------------------------------------------------

def get_job_details(url, headers, cause):
    """
    Queries the API to return a job status for the last invocation of a job
    """

    # build payload
    req_payload = {'cause': cause}


    run_job_resp = requests.get(url, headers=headers, data=req_payload).json()
    # return all the data for this job:
    # pprint(run_job_resp['data'])

    return run_job_resp['data']

def main():
  print('Beginning request for job run...')
  print(f'{req_job_url}')
  print('...')
  print('..')
  print('.')
  print(' ')

  # get job details
  job_json = get_job_details(req_job_url, req_auth_header, job_cause)

  # if you want all the things:
  # pprint(job_json)
  
  pprint(f"Job Name: {job_json[0]['job']['name']}")
  pprint(f"Kicked Off Timestamp: {job_json[0]['created_at']}")
  pprint(f"Trigger Cause: {job_json[0]['trigger']['cause']}")
  pprint(f"Steps Override: {job_json[0]['trigger']['steps_override']}")
  pprint('')
  pprint(f"Status: {job_json[0]['status_humanized']}")
  pprint(f"Status Message: {job_json[0]['status_message']}")
  pprint(f"finished_at: {job_json[0]['finished_at']}")
  pprint(f"in_progress: {job_json[0]['in_progress']}")
  pprint(f"is_cancelled: {job_json[0]['is_cancelled']}")
  pprint(f"is_complete: {job_json[0]['is_complete']}")
  pprint(f"is_error: {job_json[0]['is_error']}")
  pprint(f"is_running: {job_json[0]['is_running']}")
  pprint(f"is_success: {job_json[0]['is_success']}")
  pprint(f"execute_steps: {job_json[0]['job']['execute_steps']}")

if __name__ == "__main__":
    # get_job_details(req_job_url, req_auth_header, job_cause)
    main()
