import requests
import json
import os

from pprint import pprint

#------------------------------------------------------------------------------
# get environment variables
#------------------------------------------------------------------------------
api_base        = os.getenv('DBT_URL', 'https://cloud.getdbt.com') # default to multitenant url
api_key         = os.environ['DBT_API_KEY']  # no default here, just throw an error here if key not provided
account_id      = os.environ['DBT_ACCOUNT_ID'] # no default here, just throw an error here if id not provided
project_id      = os.environ['DBT_PROJECT_ID'] # no default here, just throw an error here if id not provided
environment_id  = os.environ['DBT_ENVIRONMENT_ID'] # no default here, just throw an error here if id not provided
job_id          = os.environ['DBT_PR_JOB_ID'] # no default here, just throw an error here if id not provided

print(f"""
Configuration:
api_base: {api_base}
account_id: {account_id}
project_id: {project_id}
job_id: {job_id}
environment_id: {environment_id}
"""
)
#------------------------------------------------------------------------------

def get_latest_run():
    headers = {'Authorization': f'Token {api_key}','Content-Type': 'application/json'}
    url = f'{api_base}/api/v2/accounts/{account_id}/runs/'
    querystring = {"job_definition_id": {job_id}
                    , "project_id": {project_id}
                    , "environment_id": {environment_id}
                    , "order_by":"-id"
                    , "status": 10
                    , "limit" : 1
                }
    # API endpoint to get the latest run for a specific job
    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()
    runs = response.json()
    # pprint(f"List of runs in JSON format: {runs}")
    if not runs['data']:
        pprint("No runs found for the specified job.")
        return []

    latest_run_id = runs['data'][0]['id']
    pprint(f"Latest run id: ")
    pprint(latest_run_id)
    pprint('=================')
    return latest_run_id


def invoke_dbt_discovery_api(api_key, latest_run_id):
    """
    This function invokes dbt Cloud API to obtain the response
    """
    variables_for_query = {
        "jobId": job_id,
        "runId": latest_run_id
    }

    # Set the GraphQL query
    gql_query = """
        query Query($jobId: BigInt!, $runId: BigInt) {
        job(id: $jobId, runId: $runId) {
            models {
            name
            executeCompletedAt
            parentsSources {
                name
                database
                schema
            }
            status
            }
        }
        }
    """

    # Set the GraphQL endpoint URL - Replace your URL, if different
    endpoint_url = "https://metadata.cloud.getdbt.com/graphql"

    response = requests.post(
        endpoint_url,
        headers={"authorization": "Bearer "+api_key, "content-type": "application/json"},
        json={"query": gql_query, "variables": variables_for_query}
        )
    
    if response.status_code == 200:
        run_data = response.json()['data']['job']['models']
        # pprint(run_data)
        return run_data
    else:
        return "Error"


def get_source_info(run_data):
    sources = set()
    for node in run_data:
        if node['status'] == 'success':
            # pprint(node))
            for parentSource in node['parentsSources']:
                # pprint(parentSource)
                sources.add(f"{parentSource['database']}.{parentSource['schema']}.{parentSource['name']}")
    pprint(f"Source tables for the latest run:")
    pprint(sources)
    return list(sources)

def main():
    try:
        latest_run_id = get_latest_run()
        run_data = invoke_dbt_discovery_api(api_key, latest_run_id)
        get_source_info(run_data)

    except requests.exceptions.RequestException as e:
        pprint(f"Error fetching data from dbt Cloud API: {e}")
        raise
    except Exception as e:
        pprint(f"An unexpected error occurred: {e}")
        raise

if __name__ == "__main__":
    main()
