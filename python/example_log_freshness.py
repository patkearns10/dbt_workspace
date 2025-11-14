"""
Example usage of the log_freshness module.
This demonstrates how to use the DBTFreshnessLogger programmatically.
"""

from log_freshness import DBTFreshnessLogger
import json


def example_basic_usage():
    """Basic usage example."""
    print("="*80)
    print("EXAMPLE 1: Basic Usage")
    print("="*80)
    
    # Initialize the logger
    logger = DBTFreshnessLogger(
        api_base="https://cloud.getdbt.com",
        api_key="your-api-key",
        account_id="123456",
        run_id="987654"
    )
    
    # Process and get results as JSON
    results = logger.process_and_log(output_format='json', write_to_db=False)
    
    print(f"\nProcessed {len(results)} items")
    return results


def example_filter_by_configuration():
    """Example showing how to filter results by freshness configuration."""
    print("\n" + "="*80)
    print("EXAMPLE 2: Filter by Freshness Configuration")
    print("="*80)
    
    logger = DBTFreshnessLogger(
        api_base="https://cloud.getdbt.com",
        api_key="your-api-key",
        account_id="123456",
        run_id="987654"
    )
    
    # Get all results
    results = logger.process_and_log(output_format='json', write_to_db=False)
    
    # Filter for items with freshness configured
    configured_items = [r for r in results if r['is_freshness_configured']]
    print(f"\nItems with freshness configured: {len(configured_items)}")
    
    # Filter for items without freshness
    not_configured_items = [r for r in results if not r['is_freshness_configured']]
    print(f"Items without freshness: {len(not_configured_items)}")
    
    # Filter for items with build_after
    build_after_items = [r for r in results if r.get('build_after_count') is not None]
    print(f"Items with build_after: {len(build_after_items)}")
    
    return configured_items, not_configured_items, build_after_items


def example_analyze_freshness_types():
    """Example showing how to analyze different freshness configuration types."""
    print("\n" + "="*80)
    print("EXAMPLE 3: Analyze Freshness Configuration Types")
    print("="*80)
    
    logger = DBTFreshnessLogger(
        api_base="https://cloud.getdbt.com",
        api_key="your-api-key",
        account_id="123456",
        run_id="987654"
    )
    
    results = logger.process_and_log(output_format='json', write_to_db=False)
    
    # Count different configuration patterns
    has_warn_after = len([r for r in results if r.get('warn_after_count') is not None])
    has_error_after = len([r for r in results if r.get('error_after_count') is not None])
    has_build_after = len([r for r in results if r.get('build_after_count') is not None])
    has_updates_on = len([r for r in results if r.get('updates_on') is not None])
    
    print("\nFreshness configuration usage:")
    print(f"  With warn_after: {has_warn_after}")
    print(f"  With error_after: {has_error_after}")
    print(f"  With build_after: {has_build_after}")
    print(f"  With updates_on: {has_updates_on}")
    
    # Analyze build_after periods
    build_periods = {}
    for result in results:
        if result.get('build_after_period'):
            period = result['build_after_period']
            build_periods[period] = build_periods.get(period, 0) + 1
    
    if build_periods:
        print("\nBuild after periods:")
        for period, count in sorted(build_periods.items(), key=lambda x: x[1], reverse=True):
            print(f"  {period}: {count}")
    
    return {
        'warn_after': has_warn_after,
        'error_after': has_error_after,
        'build_after': has_build_after,
        'updates_on': has_updates_on,
        'build_periods': build_periods
    }


def example_export_to_csv():
    """Example showing how to export to CSV file."""
    print("\n" + "="*80)
    print("EXAMPLE 4: Export to CSV File")
    print("="*80)
    
    import csv
    
    logger = DBTFreshnessLogger(
        api_base="https://cloud.getdbt.com",
        api_key="your-api-key",
        account_id="123456",
        run_id="987654"
    )
    
    results = logger.process_and_log(output_format='json', write_to_db=False)
    
    # Write to CSV file
    csv_filename = 'freshness_log.csv'
    with open(csv_filename, 'w', newline='') as csvfile:
        fieldnames = ['unique_id', 'resource_type', 'name', 'freshness_source', 
                     'freshness_keywords', 'freshness_config']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for row in results:
            csv_row = row.copy()
            csv_row['freshness_keywords'] = ','.join(row.get('freshness_keywords') or [])
            csv_row['freshness_config'] = json.dumps(row.get('freshness_config'), default=str)
            writer.writerow(csv_row)
    
    print(f"\n✓ Exported {len(results)} rows to {csv_filename}")
    return csv_filename


def example_summary_report():
    """Example showing how to create a summary report."""
    print("\n" + "="*80)
    print("EXAMPLE 5: Summary Report")
    print("="*80)
    
    logger = DBTFreshnessLogger(
        api_base="https://cloud.getdbt.com",
        api_key="your-api-key",
        account_id="123456",
        run_id="987654"
    )
    
    results = logger.process_and_log(output_format='json', write_to_db=False)
    
    # Generate summary
    summary = {
        'total_items': len(results),
        'by_resource_type': {},
        'with_freshness_config': 0,
        'without_freshness_config': 0,
        'freshness_patterns': {
            'warn_after_only': 0,
            'error_after_only': 0,
            'build_after': 0,
            'both_warn_and_error': 0
        }
    }
    
    for result in results:
        # Count by resource type
        resource_type = result.get('resource_type', 'unknown')
        summary['by_resource_type'][resource_type] = summary['by_resource_type'].get(resource_type, 0) + 1
        
        # Count with/without config
        if result.get('is_freshness_configured'):
            summary['with_freshness_config'] += 1
            
            # Analyze patterns
            has_warn = result.get('warn_after_count') is not None
            has_error = result.get('error_after_count') is not None
            has_build = result.get('build_after_count') is not None
            
            if has_warn and has_error:
                summary['freshness_patterns']['both_warn_and_error'] += 1
            elif has_warn:
                summary['freshness_patterns']['warn_after_only'] += 1
            elif has_error:
                summary['freshness_patterns']['error_after_only'] += 1
            
            if has_build:
                summary['freshness_patterns']['build_after'] += 1
        else:
            summary['without_freshness_config'] += 1
    
    # Calculate percentages
    total = summary['total_items']
    pct_with = (summary['with_freshness_config'] / total * 100) if total > 0 else 0
    
    print("\nSummary Report:")
    print(f"  Total Items: {summary['total_items']}")
    print(f"\n  By Resource Type:")
    for rtype, count in sorted(summary['by_resource_type'].items()):
        pct = (count / total * 100) if total > 0 else 0
        print(f"    {rtype}: {count} ({pct:.1f}%)")
    print(f"\n  Freshness Coverage:")
    print(f"    With Config: {summary['with_freshness_config']} ({pct_with:.1f}%)")
    print(f"    Without Config: {summary['without_freshness_config']} ({100-pct_with:.1f}%)")
    print(f"\n  Freshness Patterns (of configured items):")
    for pattern, count in summary['freshness_patterns'].items():
        print(f"    {pattern}: {count}")
    
    return summary


if __name__ == "__main__":
    print("="*80)
    print("LOG FRESHNESS - USAGE EXAMPLES")
    print("="*80)
    print("\nNote: Replace API credentials with actual values before running")
    print("These examples demonstrate the various ways to use the logger\n")
    
    # Uncomment the examples you want to run:
    
    # example_basic_usage()
    # example_filter_by_configuration()
    # example_analyze_freshness_types()
    # example_export_to_csv()
    # example_summary_report()
    
    print("\n" + "="*80)
    print("Uncomment the examples in this file to run them")
    print("="*80)

