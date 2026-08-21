-- ============================================================================
-- Grant an EXISTING Streamlit in Snowflake app access to the dbt Discovery API
-- ============================================================================
-- Streamlit in Snowflake cannot make outbound HTTP calls unless the app is
-- granted an EXTERNAL ACCESS INTEGRATION. This script creates:
--
--   1. A NETWORK RULE allowlisting the dbt Discovery API host
--   2. A SECRET holding the dbt service token
--   3. An EXTERNAL ACCESS INTEGRATION tying the two together
--   4. Grants
--   5. An ALTER STREAMLIT attaching both to the app that already exists
--
-- Run as ACCOUNTADMIN (or a role with CREATE INTEGRATION on the account).
-- Replace every <PLACEHOLDER> before running.
--
-- !! READ STEP 0 FIRST. Step 5 uses SET, which REPLACES the app's existing
-- !! integration and secret lists rather than appending to them.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

SET db      = '<YOUR_DB>';
SET sch     = '<YOUR_SCHEMA>';
SET approle = '<ROLE_THAT_OWNS_THE_APP>';   -- e.g. ANALYTICS_ENGINEER

USE DATABASE IDENTIFIER($db);
USE SCHEMA IDENTIFIER($sch);


-- ----------------------------------------------------------------------------
-- 0. Record what the app already has  <-- DO NOT SKIP
-- ----------------------------------------------------------------------------
-- ALTER STREAMLIT ... SET EXTERNAL_ACCESS_INTEGRATIONS = (...) replaces the
-- whole list. If the app already uses other integrations or secrets and you
-- omit them in step 5, you will silently break whatever depended on them.
--
-- Run these two and keep the output. You will need it in step 5.

DESCRIBE STREAMLIT <YOUR_EXISTING_APP>;
SHOW STREAMLITS LIKE '<YOUR_EXISTING_APP>';

-- Look for the external_access_integrations and secrets rows. If they are
-- empty, step 5 as written is safe. If they are not, add the existing values
-- to the lists in step 5 alongside the new ones.


-- ----------------------------------------------------------------------------
-- 1. Network rule — allow egress to your Discovery API host
-- ----------------------------------------------------------------------------
-- Host format (multi-tenant): <ACCOUNT_PREFIX>.metadata.<REGION>.dbt.com
--   REGION is us1 (North America AWS), eu1 (EMEA), au1 (APAC), jp1 (Japan), ...
-- Find the exact URL in dbt platform: Account settings -> Access URLs.
-- Port 443 must be included.

CREATE OR REPLACE NETWORK RULE dbt_discovery_api_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('<ACCOUNT_PREFIX>.metadata.<REGION>.dbt.com:443');


-- ----------------------------------------------------------------------------
-- 2. Secret — the dbt service token
-- ----------------------------------------------------------------------------
-- In dbt platform: Account settings -> Service tokens -> New token.
-- Permission set: "Metadata Only" is sufficient for the Discovery API.
-- Keep the token out of version control; paste it at run time only.
--
-- The name here must match what dbt_lineage.configure(secret_name=...) expects.
-- Default in the module is 'dbt_metadata_token'.

CREATE OR REPLACE SECRET dbt_metadata_token
  TYPE = GENERIC_STRING
  SECRET_STRING = '<DBT_SERVICE_TOKEN>';


-- ----------------------------------------------------------------------------
-- 3. External access integration
-- ----------------------------------------------------------------------------

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dbt_discovery_api_integration
  ALLOWED_NETWORK_RULES = (dbt_discovery_api_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (dbt_metadata_token)
  ENABLED = TRUE;


-- ----------------------------------------------------------------------------
-- 4. Grants
-- ----------------------------------------------------------------------------

GRANT USAGE ON INTEGRATION dbt_discovery_api_integration TO ROLE IDENTIFIER($approle);
GRANT READ ON SECRET dbt_metadata_token                  TO ROLE IDENTIFIER($approle);


-- ----------------------------------------------------------------------------
-- 5. Attach both to the existing app
-- ----------------------------------------------------------------------------
-- If step 0 showed the app already has integrations or secrets, extend these
-- lists to include them. Example of preserving an existing one:
--
--   EXTERNAL_ACCESS_INTEGRATIONS = (
--     some_existing_integration,
--     dbt_discovery_api_integration
--   )
--   SECRETS = (
--     'some_existing_secret' = <YOUR_DB>.<YOUR_SCHEMA>.some_existing_secret,
--     'dbt_metadata_token'   = <YOUR_DB>.<YOUR_SCHEMA>.dbt_metadata_token
--   )

ALTER STREAMLIT <YOUR_EXISTING_APP> SET
  EXTERNAL_ACCESS_INTEGRATIONS = (dbt_discovery_api_integration)
  SECRETS = ('dbt_metadata_token' = <YOUR_DB>.<YOUR_SCHEMA>.dbt_metadata_token);


-- ----------------------------------------------------------------------------
-- 6. Add dbt_lineage.py and the `requests` package
-- ----------------------------------------------------------------------------
-- Upload the module next to the app's existing main file:
--
--   PUT file://dbt_lineage.py @<YOUR_DB>.<YOUR_SCHEMA>.<YOUR_APP_STAGE>
--     AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
--
-- The module needs `requests`. pandas and streamlit are almost certainly
-- already present.
--   * Snowsight editor: add `requests` via the Packages menu
--   * Stage-managed app: add `requests` to the app's environment.yml
--
-- No graphviz package is needed — the module passes DOT text to
-- st.graphviz_chart.


-- ----------------------------------------------------------------------------
-- Verification
-- ----------------------------------------------------------------------------
-- Confirm the integration and secret are attached, and that nothing that was
-- there before went missing:
--
--   DESCRIBE STREAMLIT <YOUR_EXISTING_APP>;
--   SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'dbt_discovery_api_integration';
--
-- If the app reports a network error, the integration is not attached.
-- If the app reports 403 Forbidden, dbt platform IP restrictions are blocking
-- Snowflake's egress. Add your account's egress CIDRs to the dbt allowlist
-- (Account settings -> IP restrictions).
--
-- To rotate the token later — no app change needed:
--   ALTER SECRET dbt_metadata_token SET SECRET_STRING = '<NEW_TOKEN>';
--
-- To roll back cleanly:
--   ALTER STREAMLIT <YOUR_EXISTING_APP> UNSET SECRETS;
--   ALTER STREAMLIT <YOUR_EXISTING_APP> UNSET EXTERNAL_ACCESS_INTEGRATIONS;
--   DROP EXTERNAL ACCESS INTEGRATION dbt_discovery_api_integration;
--   DROP SECRET dbt_metadata_token;
--   DROP NETWORK RULE dbt_discovery_api_rule;
