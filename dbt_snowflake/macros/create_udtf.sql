{% macro create_udtf() %}
    CREATE OR REPLACE FUNCTION hw()
    RETURNS TABLE(msg VARCHAR)
    AS
    $$
        SELECT 'Hello'
        UNION
        SELECT 'World'
    $$;
{% endmacro %}