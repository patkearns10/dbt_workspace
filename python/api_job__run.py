import requests
import os
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
req_job_url = f'{api_base}/api/v2/accounts/{account_id}/jobs/{job_id}/run/'
req_payload = {'cause': job_cause}
#------------------------------------------------------------------------------

def run_job(url, headers, cause):
  """
  Runs a dbt job
  """
  # trigger job
  print(f'Triggering job:\n\turl: {url}\n\tpayload: {req_payload}')
  try:
    response = requests.post(url, headers=headers, data=req_payload)
    response.raise_for_status()
    run_job_resp = response.json()
    print('Success!')
    print(run_job_resp)
  except Exception as e:
    print(f'ERROR! - Could not trigger job:\n {e}')
    raise


def main():
  print('Beginning request for job run...')
  run_job(req_job_url, req_auth_header, req_payload)


if __name__ == "__main__":
    main()
