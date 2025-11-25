import os
import requests
import json
from collections import defaultdict
from typing import Dict, List, Set

# Configuration
DBT_CLOUD_API_BASE = os.getenv("DBT_HOST_URL")
API_KEY = os.getenv("DBT_API_KEY")
ACCOUNT_ID = os.getenv("DBT_ACCOUNT_ID")
ENVIRONMENT_ID = os.getenv("DBT_ENVIRONMENT_ID")  # Optional


def auth_headers():
    """Helper to make authorization headers."""
    return {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }


def list_jobs(account_id: str, environment_id: str = None, limit: int = 100):
    """List all jobs, optionally filtered by environment."""
    endpoint = f"{DBT_CLOUD_API_BASE}/api/v2/accounts/{account_id}/jobs/"
    params = {"limit": limit}
    
    if environment_id:
        params["environment_id"] = environment_id
    
    resp = requests.get(endpoint, headers=auth_headers(), params=params)
    resp.raise_for_status()
    return resp.json()["data"]


def get_latest_run_for_job(account_id: str, job_id: int):
    """Get the most recent completed run for a job."""
    endpoint = f"{DBT_CLOUD_API_BASE}/api/v2/accounts/{account_id}/runs/"
    params = {
        "job_definition_id": job_id,
        "limit": 1,
        "order_by": "-id",
        "status": "10"  # 10 = Success
    }
    
    resp = requests.get(endpoint, headers=auth_headers(), params=params)
    resp.raise_for_status()
    runs = resp.json()["data"]
    
    return runs[0] if runs else None


def get_run_results(account_id: str, run_id: int):
    """Download run_results.json for a specific run."""
    artifacts_url = f"{DBT_CLOUD_API_BASE}/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/"
    
    # Check if run_results.json exists
    resp = requests.get(artifacts_url, headers=auth_headers())
    resp.raise_for_status()
    artifacts = resp.json().get("data", [])
    
    # Download run_results.json
    for path in artifacts:
        if path.endswith("run_results.json"):
            res = requests.get(f"{artifacts_url}run_results.json", headers=auth_headers())
            if res.ok:
                return res.json()
    
    return None


def extract_models_from_run_results(run_results: dict) -> List[str]:
    """Extract model unique_ids from run_results.json."""
    if not run_results:
        return []
    
    models = []
    for result in run_results.get("results", []):
        unique_id = result.get("unique_id", "")
        # Filter for models (exclude tests, seeds, snapshots, etc.)
        if unique_id.startswith("model."):
            models.append(unique_id)
    
    return models


def analyze_model_job_overlap(account_id: str, environment_id: str = None):
    """
    Main analysis function that identifies models run in multiple jobs.
    
    Returns:
        - model_to_jobs: Dict mapping model unique_ids to list of job names
        - job_to_models: Dict mapping job names to list of model unique_ids
    """
    print("Fetching jobs...")
    jobs = list_jobs(account_id, environment_id)
    print(f"Found {len(jobs)} jobs\n")
    
    model_to_jobs = defaultdict(list)
    job_to_models = {}
    jobs_analyzed = 0
    jobs_skipped = 0
    
    for job in jobs:
        job_id = job["id"]
        job_name = job["name"]
        
        print(f"Analyzing Job: {job_name} (ID: {job_id})...")
        
        # Get latest successful run
        try:
            latest_run = get_latest_run_for_job(account_id, job_id)
            
            if not latest_run:
                print(f"  ⚠️  No successful runs found for job {job_name}")
                jobs_skipped += 1
                continue
            
            run_id = latest_run["id"]
            print(f"  Latest run ID: {run_id}")
            
            # Get run results
            run_results = get_run_results(account_id, run_id)
            
            if not run_results:
                print(f"  ⚠️  No run_results.json found for run {run_id}")
                jobs_skipped += 1
                continue
            
            # Extract models
            models = extract_models_from_run_results(run_results)
            print(f"  Found {len(models)} models executed")
            
            # Store in mappings
            job_to_models[job_name] = models
            for model in models:
                model_to_jobs[model].append({
                    "job_name": job_name,
                    "job_id": job_id,
                    "run_id": run_id
                })
            
            jobs_analyzed += 1
            
        except Exception as e:
            print(f"  ❌ Error processing job {job_name}: {e}")
            jobs_skipped += 1
            continue
        
        print()
    
    print(f"\n{'='*80}")
    print(f"Analysis complete!")
    print(f"Jobs analyzed: {jobs_analyzed}")
    print(f"Jobs skipped: {jobs_skipped}")
    print(f"{'='*80}\n")
    
    return dict(model_to_jobs), job_to_models


def find_overlapping_models(model_to_jobs: Dict[str, List[dict]]) -> Dict[str, List[dict]]:
    """Filter to only models that appear in multiple jobs."""
    return {
        model: jobs 
        for model, jobs in model_to_jobs.items() 
        if len(jobs) > 1
    }


def display_overlap_report(overlapping_models: Dict[str, List[dict]]):
    """Display a report of models that appear in multiple jobs."""
    if not overlapping_models:
        print("✅ No models are being run in multiple jobs!")
        return
    
    print(f"\n{'='*80}")
    print(f"MODELS RUN IN MULTIPLE JOBS")
    print(f"{'='*80}\n")
    
    # Sort by number of jobs (most overlapped first)
    sorted_models = sorted(
        overlapping_models.items(), 
        key=lambda x: len(x[1]), 
        reverse=True
    )
    
    for model_id, jobs in sorted_models:
        model_name = model_id.split(".")[-1]  # Get just the model name
        print(f"📊 Model: {model_name}")
        print(f"   Full ID: {model_id}")
        print(f"   Found in {len(jobs)} jobs:")
        
        for job_info in jobs:
            print(f"      • {job_info['job_name']} (Job ID: {job_info['job_id']}, Run ID: {job_info['run_id']})")
        
        print()


def save_results(model_to_jobs: dict, job_to_models: dict, overlapping_models: dict):
    """Save analysis results to JSON files."""
    
    # Save full mappings
    with open("model_to_jobs_mapping.json", "w") as f:
        json.dump(model_to_jobs, f, indent=2)
    print("✅ Saved model-to-jobs mapping to: model_to_jobs_mapping.json")
    
    with open("job_to_models_mapping.json", "w") as f:
        json.dump(job_to_models, f, indent=2)
    print("✅ Saved job-to-models mapping to: job_to_models_mapping.json")
    
    # Save overlapping models
    with open("overlapping_models.json", "w") as f:
        json.dump(overlapping_models, f, indent=2)
    print("✅ Saved overlapping models to: overlapping_models.json")


def main():
    """Main execution function."""
    if not all([DBT_CLOUD_API_BASE, API_KEY, ACCOUNT_ID]):
        print("Error: Missing required environment variables.")
        print("Please ensure DBT_HOST_URL, DBT_API_KEY, and DBT_ACCOUNT_ID are set.")
        return
    
    try:
        # Run analysis
        model_to_jobs, job_to_models = analyze_model_job_overlap(ACCOUNT_ID, ENVIRONMENT_ID)
        
        # Find overlapping models
        overlapping_models = find_overlapping_models(model_to_jobs)
        
        # Display report
        display_overlap_report(overlapping_models)
        
        # Save results
        save_results(model_to_jobs, job_to_models, overlapping_models)
        
        # Summary statistics
        print(f"\n{'='*80}")
        print(f"SUMMARY")
        print(f"{'='*80}")
        print(f"Total unique models found: {len(model_to_jobs)}")
        print(f"Models run in multiple jobs: {len(overlapping_models)}")
        print(f"Total jobs analyzed: {len(job_to_models)}")
        print(f"{'='*80}\n")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()