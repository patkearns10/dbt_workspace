"""
Quick diagnostic script to inspect manifest structure for freshness configs.
This helps understand where freshness configs are actually stored.
"""

import requests
import os
import json


def inspect_manifest():
    """Fetch and inspect manifest for freshness config locations."""
    
    # Get config from environment
    api_base = os.getenv('DBT_URL', 'https://cloud.getdbt.com')
    api_key = os.environ.get('DBT_API_KEY')
    account_id = os.environ.get('DBT_ACCOUNT_ID')
    run_id = os.environ.get('DBT_RUN_ID')
    
    if not all([api_key, account_id, run_id]):
        print("ERROR: Missing required environment variables")
        print("Required: DBT_API_KEY, DBT_ACCOUNT_ID, DBT_RUN_ID")
        return
    
    print(f"Fetching manifest for run {run_id}...")
    
    # Fetch manifest
    url = f'{api_base}/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/manifest.json'
    headers = {'Authorization': f'Token {api_key}'}
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    manifest = response.json()
    
    print(f"\nManifest fetched successfully!")
    print(f"Total nodes: {len(manifest.get('nodes', {}))}")
    print(f"Total sources: {len(manifest.get('sources', {}))}")
    
    # Look for nodes with freshness in config
    print("\n" + "="*80)
    print("SEARCHING FOR FRESHNESS CONFIGURATIONS")
    print("="*80)
    
    nodes_with_freshness_in_config = []
    nodes_with_freshness_direct = []
    
    for unique_id, node in manifest.get('nodes', {}).items():
        config = node.get('config', {})
        
        # Check config.freshness
        if 'freshness' in config and config['freshness']:
            nodes_with_freshness_in_config.append((unique_id, node))
        
        # Check node.freshness
        if 'freshness' in node and node['freshness']:
            nodes_with_freshness_direct.append((unique_id, node))
    
    print(f"\nNodes with config.freshness: {len(nodes_with_freshness_in_config)}")
    print(f"Nodes with direct freshness: {len(nodes_with_freshness_direct)}")
    
    # Show examples
    if nodes_with_freshness_in_config:
        print("\n" + "-"*80)
        print("EXAMPLE: Node with config.freshness")
        print("-"*80)
        unique_id, node = nodes_with_freshness_in_config[0]
        print(f"Unique ID: {unique_id}")
        print(f"Name: {node.get('name')}")
        print(f"Resource Type: {node.get('resource_type')}")
        print(f"\nFreshness Config:")
        print(json.dumps(node['config']['freshness'], indent=2))
        
        # Show the full config keys to understand structure
        print(f"\nAll config keys: {list(node.get('config', {}).keys())}")
    
    if nodes_with_freshness_direct:
        print("\n" + "-"*80)
        print("EXAMPLE: Node with direct freshness")
        print("-"*80)
        unique_id, node = nodes_with_freshness_direct[0]
        print(f"Unique ID: {unique_id}")
        print(f"Name: {node.get('name')}")
        print(f"Resource Type: {node.get('resource_type')}")
        print(f"\nFreshness Config:")
        print(json.dumps(node['freshness'], indent=2))
    
    # Check sources too
    print("\n" + "="*80)
    print("CHECKING SOURCES")
    print("="*80)
    
    sources_with_freshness = []
    for unique_id, source in manifest.get('sources', {}).items():
        if 'freshness' in source and source['freshness']:
            # Check if it's actually configured (not just null values)
            freshness = source['freshness']
            error_after = freshness.get('error_after', {}) or {}
            warn_after = freshness.get('warn_after', {}) or {}
            if error_after.get('count') is not None or warn_after.get('count') is not None:
                sources_with_freshness.append((unique_id, source))
    
    print(f"Sources with freshness configured: {len(sources_with_freshness)}")
    
    if sources_with_freshness:
        print("\n" + "-"*80)
        print("EXAMPLE: Source with freshness")
        print("-"*80)
        unique_id, source = sources_with_freshness[0]
        print(f"Unique ID: {unique_id}")
        print(f"Name: {source.get('name')}")
        print(f"\nFreshness Config:")
        print(json.dumps(source['freshness'], indent=2))
    
    # Look for build_after specifically
    print("\n" + "="*80)
    print("SEARCHING FOR BUILD_AFTER CONFIGS")
    print("="*80)
    
    nodes_with_build_after = []
    for unique_id, node in manifest.get('nodes', {}).items():
        config = node.get('config', {})
        freshness = config.get('freshness') or node.get('freshness')
        
        if freshness and isinstance(freshness, dict):
            if 'build_after' in freshness:
                nodes_with_build_after.append((unique_id, node, freshness))
    
    print(f"Nodes with build_after: {len(nodes_with_build_after)}")
    
    if nodes_with_build_after:
        print("\n" + "-"*80)
        print("EXAMPLE: Node with build_after")
        print("-"*80)
        unique_id, node, freshness = nodes_with_build_after[0]
        print(f"Unique ID: {unique_id}")
        print(f"Name: {node.get('name')}")
        print(f"Resource Type: {node.get('resource_type')}")
        print(f"\nFull Freshness Config:")
        print(json.dumps(freshness, indent=2))
        print(f"\nBuild After:")
        print(json.dumps(freshness['build_after'], indent=2))
    else:
        print("\n⚠️  No nodes found with build_after configuration")
        print("This might mean:")
        print("  1. No models in this project use build_after")
        print("  2. build_after might be stored in a different location")
        print("  3. The manifest might not include this information")


if __name__ == "__main__":
    inspect_manifest()

