"""
Quick test script to verify dbt Cloud API credentials and connection.
This helps debug issues before running the full log_freshness script.

Usage:
    python test_api_connection.py
"""

import requests
import os
import sys
import json


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


def test_connection():
    """Test the dbt Cloud API connection with current credentials."""
    
    # Get credentials from environment
    api_base = os.getenv('DBT_URL', 'https://cloud.getdbt.com')
    api_key = os.environ.get('DBT_API_KEY')
    account_id = os.environ.get('DBT_ACCOUNT_ID')
    job_id = os.environ.get('DBT_JOB_ID')
    project_id = os.environ.get('DBT_PROJECT_ID')
    
    print('=' * 80)
    print('DBT CLOUD API CONNECTION TEST')
    print('=' * 80)
    print(f'API Base: {api_base}')
    print(f'Account ID: {account_id}')
    print(f'Project ID: {project_id or "Not provided"}')
    print(f'Job ID: {job_id}')
    print(f'API Key: {"✓ Provided" if api_key else "✗ Missing"}')
    print('=' * 80)
    
    if not api_key or not account_id:
        print('\n✗ ERROR: Missing required credentials')
        print('Required: DBT_API_KEY, DBT_ACCOUNT_ID')
        sys.exit(1)
    
    headers = {'Authorization': f'Token {api_key}'}
    
    # Test 1: Get account info
    print('\n[TEST 1] Testing account access...')
    try:
        url = f'{api_base}/api/v2/accounts/{account_id}/'
        print(f'URL: {url}')
        response = requests.get(url, headers=headers)
        print(f'Status: {response.status_code}')
        
        if response.status_code == 200:
            data = response.json()
            account_name = data.get('data', {}).get('name', 'Unknown')
            print(f'✓ SUCCESS: Connected to account "{account_name}"')
        else:
            print(f'✗ FAILED: {response.text}')
            return False
    except Exception as e:
        print(f'✗ ERROR: {e}')
        return False
    
    # Test 2: List recent runs
    print('\n[TEST 2] Fetching recent runs...')
    try:
        url = f'{api_base}/api/v2/accounts/{account_id}/runs/'
        params = {
            'limit': 5,
            'order_by': '-id',
        }
        print(f'URL: {url}')
        print(f'Params: {params}')
        
        response = requests.get(url, headers=headers, params=params)
        print(f'Status: {response.status_code}')
        
        if response.status_code == 200:
            data = response.json()
            runs = data.get('data', [])
            total = data.get('extra', {}).get('pagination', {}).get('total_count', 0)
            print(f'✓ SUCCESS: Found {total} total runs (showing {len(runs)})')
            
            if runs:
                print('\nRecent runs:')
                for run in runs[:5]:
                    run_id = run.get('id')
                    status = run.get('status')
                    status_name = get_status_name(status)
                    job_info = run.get('job_id') or run.get('job', {}).get('id', 'unknown')
                    created = run.get('created_at', 'unknown')
                    print(f'  - Run {run_id}: Job {job_info}, Status: {status_name}, Created: {created}')
        else:
            print(f'✗ FAILED: {response.text}')
            return False
    except Exception as e:
        print(f'✗ ERROR: {e}')
        return False
    
    # Test 3: Get specific job runs (if job_id provided)
    if job_id:
        print(f'\n[TEST 3] Fetching runs for job {job_id}...')
        try:
            url = f'{api_base}/api/v2/accounts/{account_id}/runs/'
            params = {
                'limit': 20,
                'offset': 0,
                'order_by': '-id',
                'job_definition_id': job_id,
                'include_related': '["job","trigger","environment","repository"]',
            }
            
            if project_id:
                params['project_id'] = project_id
            
            print(f'URL: {url}')
            print(f'Params: {params}')
            
            response = requests.get(url, headers=headers, params=params)
            print(f'Status: {response.status_code}')
            
            if response.status_code == 200:
                data = response.json()
                runs = data.get('data', [])
                total = data.get('extra', {}).get('pagination', {}).get('total_count', 0)
                print(f'✓ SUCCESS: Found {total} runs for job {job_id}')
                
                if runs:
                    print(f'\nShowing first {min(5, len(runs))} runs:')
                    for run in runs[:5]:
                        run_id = run.get('id')
                        status = run.get('status')
                        status_name = get_status_name(status)
                        created = run.get('created_at', 'unknown')
                        print(f'  - Run {run_id}: Status: {status_name}, Created: {created}')
                    
                    # Show first run details
                    latest_run = runs[0]
                    latest_status = latest_run.get("status")
                    latest_status_name = get_status_name(latest_status)
                    print(f'\nLatest run details:')
                    print(f'  Run ID: {latest_run.get("id")}')
                    print(f'  Status: {latest_status_name} ({latest_status})')
                    print(f'  Created: {latest_run.get("created_at")}')
                    print(f'  Finished: {latest_run.get("finished_at", "N/A")}')
                    
                    # Check if artifacts are available
                    run_id = latest_run.get('id')
                    print(f'\n[TEST 3a] Checking if artifacts are available for run {run_id}...')
                    manifest_url = f'{api_base}/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/manifest.json'
                    manifest_response = requests.get(manifest_url, headers=headers)
                    
                    if manifest_response.status_code == 200:
                        manifest = manifest_response.json()
                        model_count = len(manifest.get('nodes', {}))
                        source_count = len(manifest.get('sources', {}))
                        print(f'✓ SUCCESS: Manifest available ({model_count} nodes, {source_count} sources)')
                    else:
                        print(f'⚠ WARNING: Manifest not available (Status: {manifest_response.status_code})')
                        print(f'  This might mean the run is too old or artifacts expired')
                else:
                    print(f'\n⚠ WARNING: No runs found for job {job_id}')
                    print(f'\nPossible reasons:')
                    print(f'  1. Job ID might be incorrect')
                    print(f'  2. Job has never been run')
                    print(f'  3. Project ID filter is excluding the job')
            else:
                print(f'✗ FAILED: {response.text}')
                return False
        except Exception as e:
            print(f'✗ ERROR: {e}')
            return False
    else:
        print('\n[TEST 3] Skipped - No DBT_JOB_ID provided')
    
    print('\n' + '=' * 80)
    print('✓ ALL TESTS PASSED')
    print('=' * 80)
    print('\nYou can now run:')
    print('  python log_freshness_from_job.py')
    print('\nOr set a specific run ID and run:')
    print('  python log_freshness.py')
    
    return True


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)

