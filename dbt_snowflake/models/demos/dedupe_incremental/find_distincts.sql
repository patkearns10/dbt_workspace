{{
    config(
        enabled=False
    )
}}

-- Bigquery only -- find distinct rows
-- cool stuff https://stackoverflow.com/questions/53719148/big-query-deduplication-query-example-explanation
SELECT tt.*
FROM (
  SELECT t.id, ARRAY_AGG(t ORDER BY t.insert_time DESC LIMIT 1)[OFFSET(0)] tt
  FROM {{ ref('dedupe_incremental') }} t
  GROUP BY 1
)