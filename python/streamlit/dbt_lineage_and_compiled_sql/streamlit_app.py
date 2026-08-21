"""
Local test harness for dbt_lineage.py
=====================================

This file is NOT what gets added to BHP's existing app — `dbt_lineage.py` is.
This is a minimal host app so you can exercise the module locally before
integrating, and a worked example of the call pattern.

    export DBT_METADATA_URL="https://abc123.metadata.us1.dbt.com/graphql"
    export DBT_SERVICE_TOKEN="dbtc_..."
    export DBT_ENVIRONMENT_ID="12345"

    python check_connection.py       # validate the API first
    streamlit run streamlit_app.py   # then exercise the tiles
"""

import os

import streamlit as st

import dbt_lineage

# Only a standalone app should call this. Your existing app already does.
st.set_page_config(page_title="dbt lineage & SQL", page_icon="🔎", layout="wide")

# In BHP's app, pass the real values here instead of reading the environment.
dbt_lineage.configure(
    metadata_url=os.environ.get("DBT_METADATA_URL"),
    environment_id=int(os.environ["DBT_ENVIRONMENT_ID"])
    if os.environ.get("DBT_ENVIRONMENT_ID")
    else None,
)

st.title("dbt lineage & compiled SQL")
st.caption(
    "Local harness for `dbt_lineage.py`. In the real app these tiles render "
    "inside your existing layout."
)

# The module offers a picker, but the host app can drive this from its own
# selector instead — both tiles just take a model name or unique_id.
selected = dbt_lineage.model_picker()

if selected:
    lineage_tab, sql_tab = st.tabs(["Lineage", "Compiled SQL"])
    dbt_lineage.render_dag_tile(selected, container=lineage_tab)
    dbt_lineage.render_sql_tile(selected, container=sql_tab)

with st.sidebar:
    st.header("Harness controls")
    st.caption("The module itself never writes to the sidebar.")
    if st.button("Clear module cache"):
        dbt_lineage.clear_cache()
        st.rerun()
    st.divider()
    st.caption(
        "Layout variants to try:\n\n"
        "```python\n"
        "# side by side\n"
        "left, right = st.columns(2)\n"
        "dbt_lineage.render_dag_tile(m, container=left)\n"
        "dbt_lineage.render_sql_tile(m, container=right)\n\n"
        "# collapsed, unobtrusive\n"
        "dbt_lineage.render_dag_tile(\n"
        "    m, container=st.expander('Lineage')\n"
        ")\n"
        "```"
    )
