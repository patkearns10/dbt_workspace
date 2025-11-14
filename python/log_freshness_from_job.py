"""
This script is a wrapper around log_freshness.py that accepts a job_id instead of run_id.
It fetches the latest (or a specific) run from the job and then analyzes its freshness configuration.

Usage:
    python log_freshness_from_job.py

Environment variables required:
    - DBT_API_KEY: dbt Cloud API token
    - DBT_ACCOUNT_ID: dbt Cloud account ID
    - DBT_JOB_ID: dbt Cloud job ID to analyze
    - DBT_PROJECT_ID (optional but recommended): dbt Cloud project ID - helps with filtering
    - DBT_URL (optional): dbt Cloud URL (defaults to https://cloud.getdbt.com)
    - DBT_RUN_INDEX (optional): Which run to analyze (0=latest, 1=second latest, etc.)
    - OUTPUT_FORMAT (optional): json, csv, or dataframe
    - ONLY_SUCCESSFUL (optional): Filter to only successful runs (defaults to true)
    - USE_ANY_RUN (optional): If no successful runs found, use any run (defaults to false)
"""

import requests
import os
import sys
from log_freshness import DBTFreshnessLogger


# dbt Cloud run status codes
RUN_STATUS_CODES = {
    1: 'queued',
    2: 'starting',
    3: 'running',
    10: 'success',
    20: 'error',
    30: 'cancelled',
}


def get_status_name(status):
    """Convert status code to readable name."""
    if isinstance(status, int):
        return RUN_STATUS_CODES.get(status, f'unknown({status})')
    return status


def get_job_runs(api_base: str, api_key: str, account_id: str, job_id: str, project_id: str = None, limit: int = 10):
    """
    Fetch recent runs for a specific job.
    
    Args:
        api_base: dbt Cloud base URL
        api_key: dbt Cloud API token
        account_id: dbt Cloud account ID
        job_id: Job ID to fetch runs for
        project_id: Project ID (optional but recommended for filtering)
        limit: Maximum number of runs to fetch
        
    Returns:
        List of run objects
    """
    url = f'{api_base}/api/v2/accounts/{account_id}/runs/'
    headers = {'Authorization': f'Token {api_key}'}
    
    # Match the exact format the dbt Cloud website uses
    params = {
        'limit': limit,
        'offset': 0,
        'order_by': '-id',  # Most recent first
        'job_definition_id': job_id,
        'include_related': '["job","trigger","environment","repository"]',
    }
    
    # Add project_id if provided (helps with filtering in some dbt Cloud instances)
    if project_id:
        params['project_id'] = project_id
    
    print(f'Fetching runs for job {job_id}...')
    print(f'API URL: {url}')
    print(f'Params: {params}')
    
    response = requests.get(url, headers=headers, params=params)
    
    # Print response details for debugging
    print(f'Response Status: {response.status_code}')
    
    if response.status_code != 200:
        print(f'Response Body: {response.text}')
        response.raise_for_status()
    
    data = response.json()
    
    # Debug: print pagination info
    if 'extra' in data:
        pagination = data.get('extra', {}).get('pagination', {})
        print(f'Pagination info: {pagination}')
    
    runs = data.get('data', [])
    
    print(f'Found {len(runs)} runs for job {job_id}')
    
    # Show status distribution
    if runs:
        status_counts = {}
        for run in runs:
            status = run.get('status', 'unknown')
            status_name = get_status_name(status)
            status_counts[status_name] = status_counts.get(status_name, 0) + 1
        print(f'Status distribution: {status_counts}')
    
    # If no runs found with job_definition_id, try filtering client-side
    if len(runs) == 0:
        print(f'\nNo runs found with job_definition_id filter. Trying without filter...')
        
        # Try without the job filter to see if we can get any runs
        params_no_filter = {
            'limit': 100,
            'order_by': '-id',
        }
        
        if project_id:
            params_no_filter['project_id'] = project_id
        
        response2 = requests.get(url, headers=headers, params=params_no_filter)
        response2.raise_for_status()
        data2 = response2.json()
        all_runs = data2.get('data', [])
        
        print(f'Total runs found (all jobs): {len(all_runs)}')
        
        # Filter manually by job_id
        runs = [r for r in all_runs if str(r.get('job_id')) == str(job_id) or str(r.get('job', {}).get('id')) == str(job_id)]
        print(f'Runs matching job {job_id}: {len(runs)}')
        
        if len(all_runs) > 0 and len(runs) == 0:
            # Print sample of available job IDs
            sample_jobs = set([str(r.get('job_id') or r.get('job', {}).get('id', 'unknown')) for r in all_runs[:10]])
            print(f'Sample of available job IDs in recent runs: {sample_jobs}')
    
    return runs


def get_latest_successful_run(runs: list):
    """
    Get the latest successful run from a list of runs.
    
    Args:
        runs: List of run objects
        
    Returns:
        Run object or None
    """
    # dbt Cloud can return various success statuses
    success_statuses = ['success', 'Success', 10, '10']
    
    for run in runs:
        status = run.get('status')
        if status in success_statuses:
            return run
    return None


def main():
    """Main function to run freshness logger from a job ID."""
    # Get configuration from environment variables
    api_base = os.getenv('DBT_URL', 'https://cloud.getdbt.com')
    api_key = os.environ.get('DBT_API_KEY')
    account_id = os.environ.get('DBT_ACCOUNT_ID')
    job_id = os.environ.get('DBT_JOB_ID')
    project_id = os.environ.get('DBT_PROJECT_ID')  # Optional but recommended
    
    if not api_key or not account_id or not job_id:
        print('ERROR: Missing required environment variables')
        print('Required: DBT_API_KEY, DBT_ACCOUNT_ID, DBT_JOB_ID')
        print('Recommended: DBT_PROJECT_ID')
        sys.exit(1)
    
    # Optional configuration
    run_index = int(os.getenv('DBT_RUN_INDEX', '0'))  # 0 = latest, 1 = second latest, etc.
    output_format = os.getenv('OUTPUT_FORMAT', 'json')
    write_to_db = os.getenv('WRITE_TO_DB', 'false').lower() == 'true'
    only_successful = os.getenv('ONLY_SUCCESSFUL', 'true').lower() == 'true'
    
    print('=' * 80)
    print('DBT FRESHNESS LOGGER (FROM JOB)')
    print('=' * 80)
    print(f'API Base: {api_base}')
    print(f'Account ID: {account_id}')
    print(f'Project ID: {project_id or "Not provided"}')
    print(f'Job ID: {job_id}')
    print(f'Run Index: {run_index} (0=latest)')
    print(f'Only Successful: {only_successful}')
    print('=' * 80)
    
    # Fetch runs for the job
    try:
        runs = get_job_runs(api_base, api_key, account_id, job_id, project_id)
    except Exception as e:
        print(f'ERROR: Failed to fetch job runs: {e}')
        sys.exit(1)
    
    if not runs:
        print('ERROR: No runs found for this job')
        sys.exit(1)
    
    # Filter to successful runs if requested
    if only_successful:
        success_statuses = ['success', 'Success', 10, '10']
        successful_runs = [r for r in runs if r.get('status') in success_statuses]
        
        if not successful_runs:
            print(f'\n⚠ WARNING: No successful runs found in the {len(runs)} most recent runs')
            available_statuses = set(get_status_name(r.get("status")) for r in runs)
            print(f'Available statuses: {available_statuses}')
            print(f'\nOptions:')
            print(f'  1. Use ONLY_SUCCESSFUL=false to analyze any run status')
            print(f'  2. Increase limit to check more runs')
            print(f'  3. Check if this job has any successful runs')
            
            # Ask user to continue or exit
            use_any_run = os.getenv('USE_ANY_RUN', 'false').lower() == 'true'
            if use_any_run:
                print(f'\nUSE_ANY_RUN=true - Proceeding with most recent run regardless of status')
            else:
                sys.exit(1)
        else:
            runs = successful_runs
            print(f'Filtered to {len(runs)} successful runs')
    
    # Get the requested run (by index)
    if run_index >= len(runs):
        print(f'ERROR: Run index {run_index} out of range (only {len(runs)} runs available)')
        sys.exit(1)
    
    selected_run = runs[run_index]
    run_id = str(selected_run.get('id'))
    run_status = selected_run.get('status')
    run_status_name = get_status_name(run_status)
    run_created = selected_run.get('created_at', 'unknown')
    
    print(f'\nSelected Run:')
    print(f'  - Run ID: {run_id}')
    print(f'  - Status: {run_status_name} ({run_status})')
    print(f'  - Created: {run_created}')
    print('=' * 80)
    
    # Create logger and process
    logger = DBTFreshnessLogger(api_base, api_key, account_id, run_id)
    results = logger.process_and_log(output_format=output_format, write_to_db=write_to_db)
    
    print(f'\n✓ Successfully analyzed {len(results)} items from run {run_id}')
    
    return results


if __name__ == "__main__":
    main()

