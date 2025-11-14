"""
This script replicates the log_freshness dbt macro functionality in Python.
It fetches dbt Cloud run artifacts and extracts freshness configuration from nodes and sources.

Usage:
    python log_freshness.py

Environment variables required:
    - DBT_API_KEY: dbt Cloud API token
    - DBT_ACCOUNT_ID: dbt Cloud account ID
    - DBT_RUN_ID: dbt Cloud run ID to analyze
    - DBT_URL (optional): dbt Cloud URL (defaults to https://cloud.getdbt.com)
    - DBT_CONNECTION_STRING (optional): Database connection string for logging results
"""

import requests
import os
import json
import re
from datetime import datetime
from typing import List, Dict, Any, Optional


class DBTFreshnessLogger:
    def __init__(self, api_base: str, api_key: str, account_id: str, run_id: str):
        """
        Initialize the freshness logger.
        
        Args:
            api_base: dbt Cloud base URL
            api_key: dbt Cloud API token
            account_id: dbt Cloud account ID
            run_id: Run ID to analyze
        """
        self.api_base = api_base
        self.api_key = api_key
        self.account_id = account_id
        self.run_id = run_id
        self.headers = {'Authorization': f'Token {api_key}'}
        
    def fetch_manifest(self) -> Dict[str, Any]:
        """Fetch manifest.json artifact from dbt Cloud."""
        url = f'{self.api_base}/api/v2/accounts/{self.account_id}/runs/{self.run_id}/artifacts/manifest.json'
        print(f'Fetching manifest from: {url}')
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def fetch_run_results(self) -> Dict[str, Any]:
        """Fetch run_results.json artifact from dbt Cloud."""
        url = f'{self.api_base}/api/v2/accounts/{self.account_id}/runs/{self.run_id}/artifacts/run_results.json'
        print(f'Fetching run results from: {url}')
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def extract_freshness_fields(self, freshness_config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract individual freshness fields from config.
        
        Args:
            freshness_config: The freshness configuration object
            
        Returns:
            Dictionary with individual freshness fields
        """
        if not freshness_config:
            return {
                'warn_after_count': None,
                'warn_after_period': None,
                'error_after_count': None,
                'error_after_period': None,
                'build_after_count': None,
                'build_after_period': None,
                'updates_on': None
            }
        
        # Extract warn_after
        warn_after = freshness_config.get('warn_after', {}) or {}
        warn_after_count = warn_after.get('count') if isinstance(warn_after, dict) else None
        warn_after_period = warn_after.get('period') if isinstance(warn_after, dict) else None
        
        # Extract error_after
        error_after = freshness_config.get('error_after', {}) or {}
        error_after_count = error_after.get('count') if isinstance(error_after, dict) else None
        error_after_period = error_after.get('period') if isinstance(error_after, dict) else None
        
        # Extract build_after
        build_after = freshness_config.get('build_after', {}) or {}
        build_after_count = build_after.get('count') if isinstance(build_after, dict) else None
        build_after_period = build_after.get('period') if isinstance(build_after, dict) else None
        
        # Extract updates_on
        updates_on = freshness_config.get('updates_on')
        
        return {
            'warn_after_count': warn_after_count,
            'warn_after_period': warn_after_period,
            'error_after_count': error_after_count,
            'error_after_period': error_after_period,
            'build_after_count': build_after_count,
            'build_after_period': build_after_period,
            'updates_on': updates_on
        }
    
    def process_nodes(self, manifest: Dict[str, Any], run_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Process nodes from run results to extract freshness information.
        
        Args:
            manifest: The manifest.json data
            run_results: The run_results.json data
            
        Returns:
            List of processed node data
        """
        rows = []
        
        # Resource types to exclude from analysis
        excluded_types = ['source', 'analysis', 'operation', 'seed', 'snapshot', 'test']
        
        # Process all nodes from manifest (not just executed ones)
        for unique_id, node in manifest.get('nodes', {}).items():
            # Skip excluded resource types
            resource_type = node.get('resource_type')
            if resource_type in excluded_types:
                continue
            
            # Try multiple locations for freshness config
            # First try node.config.freshness (most common)
            config = node.get('config', {})
            freshness_config = config.get('freshness')
            
            # If not found, try node.freshness (alternative location)
            if not freshness_config:
                freshness_config = node.get('freshness')
            
            # Check if freshness is configured
            is_freshness_configured = freshness_config is not None and freshness_config != {}
            
            # Extract individual freshness fields
            freshness_fields = self.extract_freshness_fields(freshness_config)
            
            row = {
                'unique_id': node.get('unique_id'),
                'resource_type': node.get('resource_type'),
                'name': node.get('name'),
                'is_freshness_configured': is_freshness_configured,
                'freshness_config': freshness_config
            }
            
            # Add individual freshness fields
            row.update(freshness_fields)
            
            rows.append(row)
        
        return rows
    
    def process_sources(self, manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Process all sources from manifest to extract freshness information.
        
        Args:
            manifest: The manifest.json data
            
        Returns:
            List of processed source data
        """
        rows = []
        
        for unique_id, source in manifest.get('sources', {}).items():
            # Get freshness config
            freshness_config = source.get('freshness')
            
            # Check if freshness is actually set
            is_freshness_configured = False
            if freshness_config:
                error_after = freshness_config.get('error_after', {}) or {}
                warn_after = freshness_config.get('warn_after', {}) or {}
                
                # Consider it configured if either error_after or warn_after has a count
                if error_after.get('count') is not None or warn_after.get('count') is not None:
                    is_freshness_configured = True
                else:
                    freshness_config = None
            
            # Extract individual freshness fields
            freshness_fields = self.extract_freshness_fields(freshness_config)
            
            row = {
                'unique_id': source.get('unique_id'),
                'resource_type': source.get('resource_type', 'source'),
                'name': source.get('name'),
                'is_freshness_configured': is_freshness_configured,
                'freshness_config': freshness_config
            }
            
            # Add individual freshness fields
            row.update(freshness_fields)
            
            rows.append(row)
        
        return rows
    
    def log_to_database(self, rows: List[Dict[str, Any]], connection_string: Optional[str] = None):
        """
        Log results to database (placeholder - implement based on your DB).
        
        Args:
            rows: Data to log
            connection_string: Database connection string
        """
        # This is a placeholder - you would implement actual DB connection here
        # For example, using SQLAlchemy, psycopg2, snowflake-connector-python, etc.
        
        if not connection_string:
            print("No database connection string provided - skipping database insert")
            return
        
        print(f"Would insert {len(rows)} rows to database")
        # TODO: Implement actual database insertion based on your connection type
    
    def fetch_run_logs(self) -> str:
        """
        Fetch run logs from dbt Cloud API.
        
        Returns:
            String containing the run logs
        """
        # First get run details to find the run steps
        url = f'{self.api_base}/api/v2/accounts/{self.account_id}/runs/{self.run_id}/'
        params = {'include_related': '["run_steps"]'}
        headers = {'Authorization': f'Token {self.api_key}'}
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        run_steps = data.get('data', {}).get('run_steps', [])
        
        # Find the dbt build/run step (usually has "dbt" in the name)
        all_logs = []
        for step in run_steps:
            step_name = step.get('name', '').lower()
            # Look for dbt command steps
            if 'dbt' in step_name or 'invoke' in step_name:
                logs = step.get('logs', '')
                if logs:
                    all_logs.append(logs)
        
        return '\n'.join(all_logs)
    
    def parse_logs_for_status(self, logs: str, manifest: Dict[str, Any]) -> Dict[str, str]:
        """
        Parse dbt logs to extract model execution statuses.
        
        Args:
            logs: Raw log string
            manifest: Manifest data for model lookup
            
        Returns:
            Dictionary mapping unique_id to status ('success' or 'reused')
        """
        import re
        
        status_map = {}
        
        # Parse logs line by line
        for line in logs.split('\n'):
            # Look for "Reused" status
            # Format: "Reused [  0.51s] model analytics.stg_salesforce__rev_schedules"
            reused_match = re.search(r'Reused\s+\[\s*[\d.]+s\]\s+model\s+([\w.]+)', line)
            if reused_match:
                model_path = reused_match.group(1)
                # Convert analytics.model_name to model.project.model_name
                parts = model_path.split('.')
                if len(parts) >= 2:
                    model_name = parts[-1]
                    # Find in manifest
                    for node_id, node in manifest.get('nodes', {}).items():
                        if node.get('name') == model_name and node.get('resource_type') == 'model':
                            status_map[node_id] = 'reused'
                            break
            
            # Look for successful executions
            # Format: "Completed successfully [  1.23s] model analytics.model_name"
            success_match = re.search(r'(Completed|OK)\s+(successfully\s+)?\[\s*[\d.]+s\]\s+model\s+([\w.]+)', line)
            if success_match:
                model_path = success_match.group(3)
                parts = model_path.split('.')
                if len(parts) >= 2:
                    model_name = parts[-1]
                    # Find in manifest (only if not already marked as reused)
                    for node_id, node in manifest.get('nodes', {}).items():
                        if node.get('name') == model_name and node.get('resource_type') == 'model':
                            if node_id not in status_map:  # Don't overwrite 'reused'
                                status_map[node_id] = 'success'
                            break
        
        return status_map
    
    def process_run_statuses(self, manifest: Dict[str, Any], run_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process run results to extract model execution statuses.
        Now enhanced with log parsing to detect 'reused' status.
        
        Args:
            manifest: The manifest.json data
            run_results: The run_results.json data
            
        Returns:
            Dictionary with run status information
        """
        # Resource types to exclude
        excluded_types = ['source', 'analysis', 'operation', 'seed', 'snapshot', 'test']
        
        status_data = {
            'run_id': self.run_id,
            'run_started_at': run_results.get('metadata', {}).get('generated_at'),
            'models': []
        }
        
        # Fetch and parse logs to get reused status
        try:
            print('Fetching run logs to detect reused models...')
            logs = self.fetch_run_logs()
            log_status_map = self.parse_logs_for_status(logs, manifest)
            print(f'Found {sum(1 for s in log_status_map.values() if s == "reused")} reused models in logs')
        except Exception as e:
            print(f'Warning: Could not parse logs: {e}')
            log_status_map = {}
        
        # Process results
        for result in run_results.get('results', []):
            unique_id = result.get('unique_id')
            
            # Look up resource type
            resource_type = None
            if unique_id in manifest.get('nodes', {}):
                resource_type = manifest['nodes'][unique_id].get('resource_type')
            
            # Skip excluded types (and skip if resource_type is None/unknown)
            if not resource_type or resource_type in excluded_types:
                continue
            
            # Only include models (most restrictive filter)
            if resource_type != 'model':
                continue
            
            # Get status from logs if available, otherwise from run_results
            status = log_status_map.get(unique_id, result.get('status'))
            execution_time = result.get('execution_time', 0)
            
            # Get timing details
            timing = result.get('timing', [])
            started_at = None
            completed_at = None
            
            if timing:
                # Find execute timing
                for t in timing:
                    if t.get('name') == 'execute':
                        started_at = t.get('started_at')
                        completed_at = t.get('completed_at')
                        break
            
            model_data = {
                'unique_id': unique_id,
                'name': unique_id.split('.')[-1] if unique_id else 'unknown',
                'resource_type': resource_type,
                'status': status,
                'execution_time': execution_time,
                'started_at': started_at,
                'completed_at': completed_at
            }
            
            status_data['models'].append(model_data)
        
        return status_data
    
    def process_and_log(self, output_format: str = 'json', write_to_db: bool = False, include_run_statuses: bool = False):
        """
        Main method to fetch artifacts, process data, and log results.
        
        Args:
            output_format: Output format ('json', 'csv', or 'dataframe')
            write_to_db: Whether to write results to database
            include_run_statuses: Whether to include run status analysis
            
        Returns:
            Dictionary with freshness data and optionally run status data
        """
        print('=' * 80)
        print('DBT FRESHNESS LOGGER')
        print('=' * 80)
        print(f'API Base: {self.api_base}')
        print(f'Account ID: {self.account_id}')
        print(f'Run ID: {self.run_id}')
        print('=' * 80)
        
        # Fetch artifacts
        print('\n[1/4] Fetching manifest...')
        manifest = self.fetch_manifest()
        print(f'✓ Manifest fetched successfully')
        
        print('\n[2/4] Fetching run results...')
        run_results = self.fetch_run_results()
        print(f'✓ Run results fetched successfully')
        
        # Process data
        print('\n[3/4] Processing nodes...')
        node_rows = self.process_nodes(manifest, run_results)
        print(f'✓ Processed {len(node_rows)} nodes')
        
        print('\n[4/4] Processing sources...')
        source_rows = self.process_sources(manifest)
        print(f'✓ Processed {len(source_rows)} sources')
        
        # Combine results
        all_rows = node_rows + source_rows
        
        print('\n' + '=' * 80)
        print(f'SUMMARY: Total of {len(all_rows)} items processed')
        print(f'  - Nodes: {len(node_rows)}')
        print(f'  - Sources: {len(source_rows)}')
        print('=' * 80)
        
        # Process run statuses if requested
        run_status_data = None
        if include_run_statuses:
            print('\n[BONUS] Processing run statuses...')
            run_status_data = self.process_run_statuses(manifest, run_results)
            print(f'✓ Processed {len(run_status_data["models"])} model runs')
        
        # Prepare response
        response = {
            'freshness_data': all_rows,
            'run_status_data': run_status_data
        }
        
        # Output results (just freshness for backward compatibility)
        if output_format == 'json':
            print('\n[OUTPUT] JSON Format:')
            print(json.dumps(all_rows, indent=2, default=str))
        elif output_format == 'csv':
            print('\n[OUTPUT] CSV Format:')
            self._output_csv(all_rows)
        elif output_format == 'dataframe':
            print('\n[OUTPUT] DataFrame Format:')
            self._output_dataframe(all_rows)
        
        # Write to database if requested
        if write_to_db:
            connection_string = os.getenv('DBT_CONNECTION_STRING')
            self.log_to_database(all_rows, connection_string)
        
        # Return freshness data for backward compatibility, or full response if run statuses included
        if include_run_statuses:
            return response
        return all_rows
    
    def _output_csv(self, rows: List[Dict[str, Any]]):
        """Output results in CSV format."""
        import csv
        import sys
        
        if not rows:
            print("No data to output")
            return
        
        fieldnames = ['unique_id', 'resource_type', 'name', 'is_freshness_configured',
                     'warn_after_count', 'warn_after_period', 
                     'error_after_count', 'error_after_period',
                     'build_after_count', 'build_after_period', 
                     'updates_on', 'freshness_config']
        
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in rows:
            # Convert dict to string for CSV
            csv_row = row.copy()
            csv_row['freshness_config'] = json.dumps(row.get('freshness_config'), default=str)
            writer.writerow(csv_row)
    
    def _output_dataframe(self, rows: List[Dict[str, Any]]):
        """Output results as pandas DataFrame."""
        try:
            import pandas as pd
            df = pd.DataFrame(rows)
            print(df.to_string())
            return df
        except ImportError:
            print("pandas not installed - cannot output as DataFrame")
            return None


def main():
    """Main function to run the freshness logger."""
    # Get configuration from environment variables
    api_base = os.getenv('DBT_URL', 'https://cloud.getdbt.com')
    api_key = os.environ['DBT_API_KEY']
    account_id = os.environ['DBT_ACCOUNT_ID']
    run_id = os.environ['DBT_RUN_ID']
    
    # Optional configuration
    output_format = os.getenv('OUTPUT_FORMAT', 'json')  # json, csv, or dataframe
    write_to_db = os.getenv('WRITE_TO_DB', 'false').lower() == 'true'
    
    # Create logger and process
    logger = DBTFreshnessLogger(api_base, api_key, account_id, run_id)
    results = logger.process_and_log(output_format=output_format, write_to_db=write_to_db)
    
    return results


if __name__ == "__main__":
    main()

