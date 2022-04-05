-----------------------------------
--    set up development space from production    -
-----------------------------------

CREATE OR REPLACE TRANSIENT SCHEMA analytics.dbt_pkearns_staging 
    CLONE analytics.{{ company_schema }}_staging;
CREATE OR REPLACE TRANSIENT SCHEMA analytics.dbt_pkearns 
    CLONE analytics.{{ company_schema }};

---------------------------------
--    set up production space from my developer space   -
---------------------------------

CREATE OR REPLACE TRANSIENT TABLE DEV.{{ company_schema }}_STAGING.{{ table_name }} 
    CLONE ANALYTICS.DBT_PKEARNS_STAGING.{{ table_name }} ;

CREATE OR REPLACE TRANSIENT TABLE DEV.{{ company_schema }}.{{ table_name }} 
    CLONE ANALYTICS.DBT_PKEARNS.{{ table_name }} ;   

---------------------------------
--    clone VIEWS
---------------------------------
        
CREATE OR REPLACE VIEW {{ company_database }}.{{ company_schema }}.{{ view_name }} AS
SELECT * FROM {{ company_database }}.{{ company_schema }}.{{ view_name }}
