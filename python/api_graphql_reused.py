import os
import requests
import pandas as pd
import ast
import json

url = 'https://metadata.cloud.getdbt.com/graphql'


headers = {
    'Authorization': f'Bearer dbtu_wh4XyMQxxfvD-SVZHtSFb4wxq6kTpHAO9TPGykGjWpUYL80NFk'
}

query_body = '''
query Environment($environmentId: BigInt!, $first: Int, $after: String) {
  environment(id: $environmentId) {
    applied {
      models(first: $first, after: $after) {
        pageInfo {
          startCursor
          endCursor
          hasNextPage
        }
        edges {
          node {
            config
            name
            packageName
            resourceType
            executionInfo {
              lastRunGeneratedAt
              lastRunStatus
              lastSuccessJobDefinitionId
              lastSuccessRunId
              lastRunError
              executeCompletedAt
            }
          }
        }
      }
    }
  }
}
'''

def fetch_all_models(environment_id: int, page_size: int = 500):
    all_nodes = []
    has_next_page = True
    cursor = None
    page_count = 0
    
    while has_next_page:
        page_count += 1
        print(f"Fetching page {page_count}...")
        
        variables = {
            "environmentId": environment_id,
            "first": page_size
        }
        
        # Add cursor for subsequent pages
        if cursor:
            variables["after"] = cursor
        
        try:
            response = requests.post(
                url, 
                headers=headers, 
                json={"query": query_body, "variables": variables}
            )
            response.raise_for_status()
            response_data = response.json()
            
            # Check for GraphQL errors
            if "errors" in response_data:
                print(f"GraphQL errors: {response_data['errors']}")
                break
            
            models_data = response_data.get("data", {}).get("environment", {}).get("applied", {}).get("models", {})
            
            if not models_data:
                print("No models data found in response")
                break
            
            # Extract page info and edges
            page_info = models_data.get("pageInfo", {})
            edges = models_data.get("edges", [])
            
            if not edges:
                print("No more models found")
                break
            
            # Add nodes from this page
            nodes = [edge['node'] for edge in edges]
            all_nodes.extend(nodes)
            
            print(f"  Fetched {len(nodes)} models (total: {len(all_nodes)})")
            
            # Check if there are more pages
            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
            
            if has_next_page and not cursor:
                print("Warning: hasNextPage is True but no endCursor provided")
                break
                
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            break
        except KeyError as e:
            print(f"Error parsing response: {e}")
            break
    
    print(f"Completed! Fetched {len(all_nodes)} total models across {page_count} pages")
    return all_nodes
if __name__ == "__main__":
    environment_id = 672
    all_nodes = fetch_all_models(environment_id)

    # Convert to DataFrame
    if all_nodes:
        df = pd.DataFrame(all_nodes)
        df['config'] = df['config'].apply(lambda x: json.dumps(ast.literal_eval(x)) if isinstance(x, str) else json.dumps(x))
        print(f"\nDataFrame shape: {df.shape}")
        print(df.head())
    else:
        print("No data retrieved")
        df = pd.DataFrame()


    # TODO: 1. Print a table like this:
    """
    with

    xf as (
        
        select
            name,
            packageName as package_name,
            resourceType as resource_type,
            executionInfo.lastRunGeneratedAt::timestamp as last_run_generated_at,
            executionInfo.executeCompletedAt::timestamp as last_execution_at,
            datediff('hour', last_execution_at, current_timestamp) as hours_since_last_execution,
            executionInfo.lastRunStatus::varchar as last_run_status,
            executionInfo.lastSuccessJobDefinitionId::int as last_job_id,
            executionInfo.lastSuccessRunId::int as last_run_id,
            executionInfo.lastRunError::varchar as last_run_error,

            json_extract(config, '$.freshness.build_after.count')::int as build_after_count,
            json_extract_string(config, '$.freshness.build_after.period') as build_after_period,
            json_extract_string(config, '$.freshness.build_after.updates_on') as updates_on,
            json_extract_string(config, '$.materialized') as materialization,

            case
            when build_after_period = 'day'
                then build_after_count * 24
            else build_after_count
            end as expected_hours_between_runs,

            case
            when hours_since_last_execution > expected_hours_between_runs
                then true
                else false
            end as is_outside_of_slo,

            config,
            executionInfo
            
        from df
        where package_name not in ('dbt_project_evaluator')

    )

    select * from xf
    """

    # TODO: 2. Create a graph Model Distribution by Build After Configuration
    # TODO: 3. Create a graph Distribution of statues (reusued vs success vs error, etc)
    # TODO: 4. Output current total reuse percentage for the environment