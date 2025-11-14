"""
Streamlit application for analyzing dbt freshness configuration.

Usage:
    streamlit run streamlit_freshness_app.py
"""

import streamlit as st
import pandas as pd
import requests
from log_freshness import DBTFreshnessLogger
import json
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go


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


def get_job_runs(api_base: str, api_key: str, account_id: str, job_id: str, project_id: str = None, limit: int = 20):
    """Fetch recent runs for a specific job."""
    url = f'{api_base}/api/v2/accounts/{account_id}/runs/'
    headers = {'Authorization': f'Token {api_key}'}
    
    params = {
        'limit': limit,
        'offset': 0,
        'order_by': '-id',
        'job_definition_id': job_id,
        'include_related': '["job","trigger","environment","repository"]',
    }
    
    if project_id:
        params['project_id'] = project_id
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    
    data = response.json()
    runs = data.get('data', [])
    
    return runs


def analyze_run_statuses(api_base: str, api_key: str, account_id: str, job_id: str, project_id: str = None, 
                         start_date: datetime = None, end_date: datetime = None, limit: int = 100):
    """
    Analyze run statuses for a job over a date range.
    
    Returns a dataframe with run status information.
    """
    # Fetch runs
    runs = get_job_runs(api_base, api_key, account_id, job_id, project_id, limit)
    
    # Filter by date if provided
    if start_date or end_date:
        filtered_runs = []
        for run in runs:
            created_at_str = run.get('created_at')
            if created_at_str:
                try:
                    created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                    # Remove timezone for comparison
                    created_at = created_at.replace(tzinfo=None)
                    
                    if start_date and created_at < start_date:
                        continue
                    if end_date and created_at > end_date:
                        continue
                    
                    filtered_runs.append(run)
                except:
                    pass
        runs = filtered_runs
    
    # Analyze each run
    all_model_statuses = []
    
    for run in runs:
        run_id = run.get('id')
        run_created = run.get('created_at')
        
        # Fetch run status data
        logger = DBTFreshnessLogger(api_base, api_key, account_id, str(run_id))
        try:
            result = logger.process_and_log(output_format='json', write_to_db=False, include_run_statuses=True)
            
            if result and isinstance(result, dict) and 'run_status_data' in result:
                run_status_data = result['run_status_data']
                
                if run_status_data and 'models' in run_status_data:
                    for model in run_status_data['models']:
                        model['run_id'] = run_id
                        model['run_created_at'] = run_created
                        all_model_statuses.append(model)
        except Exception as e:
            st.warning(f"Could not process run {run_id}: {str(e)}")
            continue
    
    if all_model_statuses:
        return pd.DataFrame(all_model_statuses)
    return pd.DataFrame()


def calculate_summary_stats(results):
    """Calculate summary statistics for freshness usage."""
    if not results:
        return None
    
    df = pd.DataFrame(results)
    
    # Overall stats
    total_items = len(df)
    items_with_freshness = len(df[df['is_freshness_configured'] == True])
    items_without_freshness = total_items - items_with_freshness
    
    # By resource type
    resource_stats = []
    for resource_type in sorted(df['resource_type'].unique()):
        subset = df[df['resource_type'] == resource_type]
        total = len(subset)
        with_freshness = len(subset[subset['is_freshness_configured'] == True])
        pct = (with_freshness / total * 100) if total > 0 else 0
        
        resource_stats.append({
            'Resource Type': resource_type,
            'Total Count': total,
            'With Freshness': with_freshness,
            'Without Freshness': total - with_freshness,
            '% With Freshness': f'{pct:.1f}%'
        })
    
    summary = {
        'overall': {
            'total': total_items,
            'with_freshness': items_with_freshness,
            'without_freshness': items_without_freshness,
            'pct_with_freshness': (items_with_freshness / total_items * 100) if total_items > 0 else 0
        },
        'by_resource': pd.DataFrame(resource_stats)
    }
    
    return summary


def main():
    st.set_page_config(
        page_title="dbt Freshness & Run Analyzer",
        page_icon="🔍",
        layout="wide"
    )
    
    # Initialize session state for configuration
    if 'config' not in st.session_state:
        st.session_state.config = {
            'api_base': 'https://cloud.getdbt.com',
            'api_key': '',
            'account_id': '',
            'job_id': '',
            'project_id': '',
            'configured': False
        }
    
    st.title("🔍 dbt Freshness & Run Status Analyzer")
    
    # Show configuration sidebar
    show_configuration_sidebar()
    
    # Create tabs for different pages
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Summary Statistics", 
        "⚙️ Configuration",
        "📋 Freshness Details", 
        "📈 Run Status Details"
    ])
    
    with tab1:
        show_summary_statistics()
    
    with tab2:
        show_configuration_page()
    
    with tab3:
        show_freshness_analysis()
    
    with tab4:
        show_run_status_analysis()


def show_configuration_sidebar():
    """Show minimal configuration status in sidebar."""
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        if st.session_state.config['configured']:
            st.success("✅ Configured")
            st.caption(f"Account: {st.session_state.config['account_id']}")
            st.caption(f"Job: {st.session_state.config['job_id']}")
            
            if st.button("🔄 Reconfigure", use_container_width=True):
                st.session_state.config['configured'] = False
                st.rerun()
        else:
            st.warning("⚠️ Not Configured")
            st.caption("Go to Configuration tab to set up")


def show_configuration_page():
    """Show configuration page for setting up credentials and common settings."""
    st.header("⚙️ Configuration")
    st.markdown("Set up your dbt Cloud credentials and default settings here. These will be used across all analysis pages.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔐 dbt Cloud Credentials")
        
        api_base = st.text_input(
            "dbt Cloud URL",
            value=st.session_state.config['api_base'],
            help="Base URL for dbt Cloud instance",
            key="config_api_base"
        )
        
        api_key = st.text_input(
            "API Key",
            value=st.session_state.config['api_key'],
            type="password",
            help="dbt Cloud API token (keep this secret!)",
            key="config_api_key"
        )
        
        account_id = st.text_input(
            "Account ID",
            value=st.session_state.config['account_id'],
            help="Your dbt Cloud account ID",
            key="config_account_id"
        )
    
    with col2:
        st.subheader("🎯 Default Job Settings")
        
        job_id = st.text_input(
            "Job ID",
            value=st.session_state.config['job_id'],
            help="Default job ID to analyze",
            key="config_job_id"
        )
        
        project_id = st.text_input(
            "Project ID (Optional)",
            value=st.session_state.config['project_id'],
            help="dbt Cloud project ID",
            key="config_project_id"
        )
    
    st.divider()
    
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        if st.button("💾 Save Configuration", type="primary", use_container_width=True):
            if not api_key or not account_id:
                st.error("❌ API Key and Account ID are required")
            else:
                st.session_state.config = {
                    'api_base': api_base,
                    'api_key': api_key,
                    'account_id': account_id,
                    'job_id': job_id,
                    'project_id': project_id,
                    'configured': True
                }
                st.success("✅ Configuration saved!")
                st.rerun()
    
    with col2:
        if st.button("🔄 Clear Configuration", use_container_width=True):
            st.session_state.config = {
                'api_base': 'https://cloud.getdbt.com',
                'api_key': '',
                'account_id': '',
                'job_id': '',
                'project_id': '',
                'configured': False
            }
            st.success("✅ Configuration cleared!")
            st.rerun()
    
    # Show current status
    if st.session_state.config['configured']:
        st.success("✅ Configuration is saved and ready to use")
    else:
        st.info("ℹ️ Fill in the required fields and click 'Save Configuration' to get started")


def show_summary_statistics():
    """Show high-level summary statistics page."""
    st.header("📊 Summary Statistics")
    st.markdown("High-level overview of your dbt project health and efficiency")
    
    # Check if configured
    if not st.session_state.config['configured']:
        st.warning("⚠️ Please configure your settings in the Configuration tab first")
        return
    
    config = st.session_state.config
    
    # Additional settings for summary
    col1, col2 = st.columns(2)
    
    with col1:
        run_mode = st.radio(
            "Analysis Mode",
            ["Latest Run", "Specific Run ID"],
            help="Analyze the latest run or specify a run ID"
        )
    
    with col2:
        if run_mode == "Specific Run ID":
            run_id = st.text_input("Run ID", help="Specific run ID to analyze")
        else:
            run_id = None
    
    if st.button("📊 Generate Summary", type="primary", use_container_width=False):
        with st.spinner("🔄 Fetching data..."):
            try:
                # Get run ID if needed
                if run_mode == "Latest Run":
                    runs = get_job_runs(
                        config['api_base'], 
                        config['api_key'], 
                        config['account_id'], 
                        config['job_id'],
                        config['project_id'] or None,
                        limit=1
                    )
                    if not runs:
                        st.error("❌ No runs found for this job")
                        return
                    run_id = runs[0]['id']
                
                if not run_id:
                    st.error("❌ Please provide a run ID")
                    return
                
                # Fetch freshness data
                logger = DBTFreshnessLogger(
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    str(run_id)
                )
                
                freshness_result = logger.process_and_log(output_format='json', write_to_db=False, include_run_statuses=True)
                
                if not freshness_result:
                    st.error("❌ Failed to fetch data")
                    return
                
                # Extract data
                if isinstance(freshness_result, dict):
                    freshness_data = freshness_result.get('freshness_data', [])
                    run_status_data = freshness_result.get('run_status_data', {})
                else:
                    freshness_data = freshness_result
                    run_status_data = {}
                
                # Calculate metrics
                st.success(f"✅ Analyzed Run ID: {run_id}")
                st.divider()
                
                # KEY METRICS
                st.subheader("🎯 Key Metrics")
                
                # Freshness Configuration Metric
                total_items = len(freshness_data)
                items_with_freshness = sum(1 for item in freshness_data if item.get('is_freshness_configured'))
                freshness_pct = (items_with_freshness / total_items * 100) if total_items > 0 else 0
                
                # Reuse Rate Metric
                models = run_status_data.get('models', [])
                total_models = len(models)
                # Check for both 'skipped' and 'reused' status
                reused_models = sum(1 for m in models if m.get('status') in ['skipped', 'reused'])
                reuse_pct = (reused_models / total_models * 100) if total_models > 0 else 0
                
                # Display metrics
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric(
                        "Freshness Configuration Coverage",
                        f"{freshness_pct:.1f}%",
                        delta=f"{freshness_pct - 80:.1f}% vs goal",
                        delta_color="normal" if freshness_pct >= 80 else "inverse"
                    )
                    
                    # Progress bar
                    st.progress(min(freshness_pct / 100, 1.0))
                    
                    if freshness_pct >= 80:
                        st.success(f"✅ **Exceeds goal of 80%**")
                    else:
                        st.warning(f"⚠️ **Below goal of 80%** (need {80 - freshness_pct:.1f}% more)")
                    
                    st.caption(f"{items_with_freshness:,} of {total_items:,} items have freshness config")
                
                with col2:
                    st.metric(
                        "Model Reuse Rate",
                        f"{reuse_pct:.1f}%",
                        delta=f"{reuse_pct - 30:.1f}% vs goal",
                        delta_color="normal" if reuse_pct >= 30 else "inverse"
                    )
                    
                    # Progress bar
                    st.progress(min(reuse_pct / 100, 1.0))
                    
                    if reuse_pct >= 30:
                        st.success(f"✅ **Exceeds goal of 30%**")
                    else:
                        st.info(f"ℹ️ **Below goal of 30%** (opportunity to optimize)")
                    
                    st.caption(f"{reused_models:,} of {total_models:,} models reused (cache hit)")
                
                st.divider()
                
                # BREAKDOWN BY RESOURCE TYPE
                st.subheader("📋 Freshness by Resource Type")
                
                df = pd.DataFrame(freshness_data)
                resource_breakdown = []
                
                for resource_type in sorted(df['resource_type'].unique()):
                    subset = df[df['resource_type'] == resource_type]
                    total = len(subset)
                    with_freshness = len(subset[subset['is_freshness_configured'] == True])
                    pct = (with_freshness / total * 100) if total > 0 else 0
                    
                    resource_breakdown.append({
                        'Resource Type': resource_type,
                        'Total': total,
                        'With Freshness': with_freshness,
                        'Coverage %': f"{pct:.1f}%"
                    })
                
                st.dataframe(
                    pd.DataFrame(resource_breakdown),
                    use_container_width=True,
                    hide_index=True
                )
                
                st.divider()
                
                # MODEL STATUS BREAKDOWN
                if models:
                    st.subheader("🔄 Model Execution Status")
                    
                    status_df = pd.DataFrame(models)
                    status_counts = status_df['status'].value_counts()
                    
                    cols = st.columns(len(status_counts))
                    for i, (status, count) in enumerate(status_counts.items()):
                        with cols[i]:
                            pct = (count / total_models * 100) if total_models > 0 else 0
                            st.metric(
                                status.title(),
                                f"{count:,}",
                                delta=f"{pct:.1f}%"
                            )
                
                st.divider()
                st.info("💡 **Tip**: Go to 'Freshness Details' or 'Run Status Details' tabs for more detailed analysis")
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                st.exception(e)


def show_freshness_analysis():
    """Show detailed freshness configuration analysis."""
    st.header("📋 Freshness Configuration Details")
    st.markdown("Detailed analysis of freshness configurations across all dbt resources")
    
    # Check if configured
    if not st.session_state.config['configured']:
        st.warning("⚠️ Please configure your settings in the Configuration tab first")
        return
    
    config = st.session_state.config
    
    # Run selection
    col1, col2 = st.columns([1, 3])
    
    with col1:
        input_type = st.radio(
            "Analysis Mode:",
            ["Latest Run", "Specific Run ID"],
            help="Choose analysis mode",
            key="freshness_input_type"
        )
    
    with col2:
        if input_type == "Specific Run ID":
            run_id = st.text_input(
                "Run ID",
                help="Specific dbt Cloud run ID to analyze",
                key="freshness_run_id"
            )
        else:
            run_id = None
    
    analyze_button = st.button("🔍 Analyze Freshness", type="primary", key="freshness_analyze")
    
    # Main content area
    if not analyze_button:
        st.info("👈 Configure your settings in the sidebar and click 'Analyze Freshness' to begin")
        
        # Show example output
        st.subheader("📊 Example Output")
        st.markdown("""
        This tool will help you:
        - 📋 View all models and sources with their freshness configuration
        - 📈 See summary statistics on freshness adoption
        - 🔎 Identify which configs come from SQL vs YAML
        - 📊 Track freshness coverage across resource types
        """)
        
        return
    
    # Validation
    if input_type == "Specific Run ID" and not run_id:
        st.error("❌ Please provide a Run ID")
        return
    
    # Process the analysis
    try:
        with st.spinner("🔄 Processing..."):
            # If using Latest Run, get it
            if input_type == "Latest Run":
                runs = get_job_runs(
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    config['job_id'],
                    config['project_id'] or None,
                    limit=1
                )
                
                if not runs:
                    st.error(f"❌ No runs found for job {config['job_id']}")
                    return
                
                run_id = runs[0]['id']
                st.info(f"📋 Analyzing latest run: {run_id}")
            
            # Now analyze the selected run
            with st.status("Analyzing freshness configuration...") as status:
                status.update(label="Fetching manifest...")
                logger = DBTFreshnessLogger(
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    str(run_id)
                )
                
                status.update(label="Processing nodes and sources...")
                results = logger.process_and_log(output_format='json', write_to_db=False)
                
                status.update(label="Analysis complete!", state="complete")
            
            # Display results
            st.success(f"✅ Successfully analyzed {len(results)} items from run {run_id}")
            
            # Calculate summary statistics
            summary = calculate_summary_stats(results)
            
            # Show summary first
            st.header("📊 Summary Statistics")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Total Items",
                    summary['overall']['total']
                )
            
            with col2:
                st.metric(
                    "With Freshness",
                    summary['overall']['with_freshness'],
                    delta=f"{summary['overall']['pct_with_freshness']:.1f}%"
                )
            
            with col3:
                st.metric(
                    "Without Freshness",
                    summary['overall']['without_freshness']
                )
            
            with col4:
                st.metric(
                    "Coverage",
                    f"{summary['overall']['pct_with_freshness']:.1f}%"
                )
            
            # Summary tables
            st.subheader("📈 Freshness Coverage by Resource Type")
            st.dataframe(
                summary['by_resource'],
                use_container_width=True,
                hide_index=True
            )
            
            # Detailed results
            st.header("📋 Detailed Results")
            
            # Convert results to DataFrame
            df = pd.DataFrame(results)
            
            # Add filters
            col1, col2 = st.columns(2)
            
            with col1:
                resource_filter = st.multiselect(
                    "Filter by Resource Type",
                    options=sorted(df['resource_type'].unique()),
                    default=df['resource_type'].unique()
                )
            
            with col2:
                has_freshness = st.selectbox(
                    "Has Freshness Config",
                    options=["All", "Yes", "No"]
                )
            
            # Apply filters
            filtered_df = df[df['resource_type'].isin(resource_filter)]
            
            if has_freshness == "Yes":
                filtered_df = filtered_df[filtered_df['is_freshness_configured'] == True]
            elif has_freshness == "No":
                filtered_df = filtered_df[filtered_df['is_freshness_configured'] == False]
            
            # Format the dataframe for display
            display_df = filtered_df.copy()
            
            # Reorder columns for better display
            display_columns = [
                'name', 'resource_type', 'is_freshness_configured',
                'warn_after_count', 'warn_after_period',
                'error_after_count', 'error_after_period',
                'build_after_count', 'build_after_period',
                'updates_on', 'unique_id'
            ]
            display_df = display_df[display_columns]
            
            # Rename columns for better display
            display_df.columns = [
                'Name', 'Resource Type', 'Has Freshness',
                'Warn Count', 'Warn Period',
                'Error Count', 'Error Period',
                'Build Count', 'Build Period',
                'Updates On', 'Unique ID'
            ]
            
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True
            )
            
            st.info(f"Showing {len(filtered_df)} of {len(df)} items")
            
            # Download buttons
            st.subheader("💾 Download Results")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Download full results as JSON
                json_str = json.dumps(results, indent=2, default=str)
                st.download_button(
                    label="📥 Download as JSON",
                    data=json_str,
                    file_name=f"freshness_analysis_run_{run_id}.json",
                    mime="application/json"
                )
            
            with col2:
                # Download as CSV
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name=f"freshness_analysis_run_{run_id}.csv",
                    mime="text/csv"
                )
            
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ API Error: {e}")
        st.error("Please check your credentials and IDs")
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        st.exception(e)


def show_run_status_analysis():
    """Show run status analysis tab."""
    st.header("📈 Run Status Details")
    st.markdown("Analyze model execution statuses (success vs reused) across multiple job runs")
    
    # Check if configured
    if not st.session_state.config['configured']:
        st.warning("⚠️ Please configure your settings in the Configuration tab first")
        return
    
    config = st.session_state.config
    
    # Analysis parameters
    st.subheader("Analysis Parameters")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Default to last 7 days (reduced for performance)
        default_start = datetime.now() - timedelta(days=7)
        default_end = datetime.now()
        
        start_date = st.date_input(
            "Start Date",
            value=default_start,
            help="Start of date range"
        )
    
    with col2:
        end_date = st.date_input(
            "End Date",
            value=default_end,
            help="End of date range"
        )
    
    with col3:
        max_runs = st.slider(
            "Max Runs",
            min_value=5,
            max_value=50,
            value=10,
            help="Maximum number of runs to analyze (fewer runs = faster)"
        )
    
    st.info("💡 **Performance Tip**: Each run requires 2 API calls (manifest + results). Start with 5-10 runs for faster analysis.")
    
    analyze_button = st.button("📊 Analyze Run Statuses", type="primary", key="run_analyze")
    
    # Main content
    if not analyze_button:
        st.info("⬆️ Set parameters and click 'Analyze Run Statuses' to begin")
        return
    
    try:
        # Convert dates to datetime
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        
        # Create progress tracking
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        status_text.text(f"🔄 Fetching runs from {start_date} to {end_date}...")
        
        # Fetch runs
        runs = get_job_runs(
            config['api_base'],
            config['api_key'],
            config['account_id'],
            config['job_id'],
            config['project_id'] or None,
            limit=max_runs
        )
        
        # Filter by date
        if start_datetime or end_datetime:
            filtered_runs = []
            for run in runs:
                created_at_str = run.get('created_at')
                if created_at_str:
                    try:
                        created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                        created_at = created_at.replace(tzinfo=None)
                        
                        if start_datetime and created_at < start_datetime:
                            continue
                        if end_datetime and created_at > end_datetime:
                            continue
                        
                        filtered_runs.append(run)
                    except:
                        pass
            runs = filtered_runs
        
        if not runs:
            st.warning("No runs found in the specified date range")
            progress_bar.empty()
            status_text.empty()
            return
        
        status_text.text(f"✅ Found {len(runs)} runs. Analyzing each run...")
        progress_bar.progress(10)
        
        # Analyze each run with progress updates
        all_model_statuses = []
        
        for i, run in enumerate(runs):
            run_id = run.get('id')
            run_created = run.get('created_at')
            
            # Update progress
            progress = 10 + int((i / len(runs)) * 80)
            progress_bar.progress(progress)
            status_text.text(f"🔄 Processing run {i+1}/{len(runs)}: {run_id}...")
            
            # Fetch run status data
            logger = DBTFreshnessLogger(
                config['api_base'],
                config['api_key'],
                config['account_id'],
                str(run_id)
            )
            try:
                result = logger.process_and_log(output_format='json', write_to_db=False, include_run_statuses=True)
                
                if result and isinstance(result, dict) and 'run_status_data' in result:
                    run_status_data = result['run_status_data']
                    
                    if run_status_data and 'models' in run_status_data:
                        for model in run_status_data['models']:
                            model['run_id'] = run_id
                            model['run_created_at'] = run_created
                            all_model_statuses.append(model)
            except Exception as e:
                st.warning(f"Could not process run {run_id}: {str(e)}")
                continue
        
        progress_bar.progress(100)
        status_text.text("✅ Analysis complete!")
        
        # Convert to dataframe
        if all_model_statuses:
            df = pd.DataFrame(all_model_statuses)
        else:
            df = pd.DataFrame()
        
        # Clear progress indicators
        progress_bar.empty()
        status_text.empty()
        
        if df.empty:
            st.warning("No run data found for the specified date range and job")
            return
        
        st.success(f"✅ Analyzed {len(df)} model executions across {df['run_id'].nunique()} runs")
        
        # Diagnostic info
        with st.expander("🔍 Status Breakdown"):
            st.markdown("**Status values found:**")
            status_breakdown = df['status'].value_counts()
            for status, count in status_breakdown.items():
                pct = (count / len(df) * 100) if len(df) > 0 else 0
                st.text(f"  {status}: {count:,} ({pct:.1f}%)")
            
            st.markdown("""
            **How we detect 'Reused' status:**  
            We parse the dbt Cloud run logs which contain lines like:
            ```
            Reused [  0.51s] model analytics.stg_salesforce__rev_schedules
            ```
            This gives us accurate reuse counts matching the dbt Cloud UI!
            """)
        
        # Summary Statistics
        st.header("📈 Summary Statistics")
        
        # Calculate status counts
        status_counts = df['status'].value_counts()
        total_executions = len(df)
        success_count = status_counts.get('success', 0)
        # Check for both 'skipped' and 'reused' status
        reused_count = status_counts.get('skipped', 0) + status_counts.get('reused', 0)
        error_count = status_counts.get('error', 0)
        
        # Calculate percentages
        success_pct = (success_count / total_executions * 100) if total_executions > 0 else 0
        reused_pct = (reused_count / total_executions * 100) if total_executions > 0 else 0
        error_pct = (error_count / total_executions * 100) if total_executions > 0 else 0
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Executions", f"{total_executions:,}")
        
        with col2:
            st.metric("Success", f"{success_count:,}", delta=f"{success_pct:.1f}%")
        
        with col3:
            st.metric("Reused", f"{reused_count:,}", delta=f"{reused_pct:.1f}%")
        
        with col4:
            st.metric("Reuse Rate", f"{reused_pct:.1f}%")
        
        # Status breakdown by run
        st.subheader("📊 Status Distribution by Run")
        
        # Group by run and status
        run_status_summary = df.groupby(['run_id', 'run_created_at', 'status']).size().reset_index(name='count')
        
        # Pivot for stacked bar chart
        pivot_df = run_status_summary.pivot_table(
            index=['run_id', 'run_created_at'],
            columns='status',
            values='count',
            fill_value=0
        ).reset_index()
        
        # Sort by date
        pivot_df['run_created_at'] = pd.to_datetime(pivot_df['run_created_at'])
        pivot_df = pivot_df.sort_values('run_created_at')
        
        # Create stacked bar chart
        fig = go.Figure()
        
        # Add bars for each status
        for status in pivot_df.columns[2:]:  # Skip run_id and run_created_at
            fig.add_trace(go.Bar(
                name=status,
                x=pivot_df['run_created_at'],
                y=pivot_df[status],
                text=pivot_df[status],
                textposition='inside'
            ))
        
        fig.update_layout(
            title="Model Execution Status by Run",
            xaxis_title="Run Date",
            yaxis_title="Number of Models",
            barmode='stack',
            hovermode='x unified',
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Detailed breakdown table
        st.subheader("📋 Detailed Run Breakdown")
        
        st.markdown("""
        **Status values** are parsed from dbt Cloud run logs to accurately identify reused vs. executed models.
        """)
        
        # Create summary by run
        run_summary = df.groupby(['run_id', 'run_created_at']).agg({
            'status': lambda x: x.value_counts().to_dict()
        }).reset_index()
        
        # Calculate execution time stats separately
        exec_time_stats = df.groupby(['run_id']).agg({
            'execution_time': ['mean', 'median']
        }).reset_index()
        exec_time_stats.columns = ['run_id', 'avg_exec_time', 'median_exec_time']
        
        # Merge execution time stats
        run_summary = run_summary.merge(exec_time_stats, on='run_id', how='left')
        
        # Expand status counts
        status_columns = []
        for status in df['status'].unique():
            run_summary[status] = run_summary['status'].apply(lambda x: x.get(status, 0))
            status_columns.append(status)
        
        # Calculate totals and percentages
        run_summary['total'] = run_summary[status_columns].sum(axis=1)
        
        # Calculate reuse rate if we have skipped or reused statuses
        has_reuse_column = False
        if 'skipped' in status_columns or 'reused' in status_columns:
            # Sum skipped and reused columns (handle if they don't exist)
            skipped_col = run_summary['skipped'] if 'skipped' in status_columns else 0
            reused_col_data = run_summary['reused'] if 'reused' in status_columns else 0
            total_reused = skipped_col + reused_col_data
            run_summary['reuse_rate_%'] = (total_reused / run_summary['total'] * 100).round(1)
            has_reuse_column = True
        
        # Format for display
        display_columns = ['run_created_at', 'run_id', 'total'] + status_columns
        if has_reuse_column:
            display_columns.append('reuse_rate_%')
        
        # Add execution time to display columns
        display_columns.extend(['avg_exec_time', 'median_exec_time'])
        
        display_summary = run_summary[display_columns].copy()
        display_summary['run_created_at'] = pd.to_datetime(display_summary['run_created_at']).dt.strftime('%Y-%m-%d %H:%M')
        
        # Round execution times
        display_summary['avg_exec_time'] = display_summary['avg_exec_time'].round(3)
        display_summary['median_exec_time'] = display_summary['median_exec_time'].round(3)
        
        # Set column names
        new_column_names = ['Run Date', 'Run ID', 'Total'] + [s.title() for s in status_columns]
        if has_reuse_column:
            new_column_names.append('Reuse Rate %')
        new_column_names.extend(['Avg Time (s)', 'Median Time (s)'])
        display_summary.columns = new_column_names
        
        st.dataframe(display_summary, use_container_width=True, hide_index=True)
        
        # Download option
        st.subheader("💾 Download Data")
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download Full Run Data as CSV",
            data=csv,
            file_name=f"run_status_analysis_{config['job_id']}_{start_date}_to_{end_date}.csv",
            mime="text/csv"
        )
        
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ API Error: {e}")
        st.error("Please check your credentials and IDs")
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        st.exception(e)


if __name__ == "__main__":
    main()

