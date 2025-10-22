WITH stg_customers AS (
  SELECT
    *
  FROM {{ ref('stg_customers') }}
), fct_orders AS (
  SELECT
    *
  FROM {{ ref('fct_orders') }}
), order_items AS (
  SELECT
    *
  FROM {{ ref('order_items') }}
), projection_7bec AS (
  SELECT
    *
    RENAME (CUSTOMER_ID AS CUSTOMERS_CUSTOMER_ID)
  FROM stg_customers
), projection_617b AS (
  SELECT
    *
    RENAME (ORDER_ID AS ORDERS_ORDER_ID, ORDERED_AT AS ORDERS_ORDERED_AT)
  FROM fct_orders
), projection_420b AS (
  SELECT
    *
    RENAME (ORDER_ID AS ORDER_ITEMS_ORDER_ID, ORDERED_AT AS ORDER_ITEMS_ORDERED_AT)
  FROM order_items
), join_36e8 AS (
  SELECT
    *
  FROM projection_617b
  LEFT JOIN projection_420b
    ON projection_617b.ORDERS_ORDER_ID = projection_420b.ORDER_ITEMS_ORDER_ID
), formula_ac1e AS (
  SELECT
    *,
    COUNT(DISTINCT ORDERS_ORDER_ID) > 1 AS IS_REPEAT_BUYER
  FROM join_36e8
), aggregation_0719 AS (
  SELECT
    CUSTOMER_ID,
    COUNT(DISTINCT ORDERS_ORDER_ID) AS COUNT_LIFETIME_ORDERS,
    MIN(ORDERS_ORDERED_AT) AS FIRST_ORDERED_AT,
    MAX(ORDERS_ORDERED_AT) AS LAST_ORDERED_AT,
    SUM(PRODUCT_PRICE) AS LIFETIME_SPEND_PRETAX,
    SUM(ORDER_TOTAL) AS LIFETIME_SPEND
  FROM formula_ac1e
  GROUP BY
    CUSTOMER_ID
), projection_ff32 AS (
  SELECT
    CUSTOMER_ID AS ORDER_SUMMARY_CUSTOMER_ID,
    COUNT_LIFETIME_ORDERS,
    IS_REPEAT_BUYER,
    FIRST_ORDERED_AT,
    LAST_ORDERED_AT,
    LIFETIME_SPEND_PRETAX,
    LIFETIME_SPEND
  FROM aggregation_0719
), join_108a AS (
  SELECT
    *
  FROM projection_7bec
  LEFT JOIN projection_ff32
    ON projection_7bec.CUSTOMERS_CUSTOMER_ID = projection_ff32.ORDER_SUMMARY_CUSTOMER_ID
), formula_1b82 AS (
  SELECT
    *,
    CASE WHEN IS_REPEAT_BUYER THEN 'returning' ELSE 'new' END AS CUSTOMER_TYPE
  FROM join_108a
), projection_66e2 AS (
  SELECT
    CUSTOMERS_CUSTOMER_ID AS CUSTOMER_ID,
    CUSTOMER_NAME,
    COUNT_LIFETIME_ORDERS,
    FIRST_ORDERED_AT,
    LAST_ORDERED_AT,
    LIFETIME_SPEND_PRETAX,
    LIFETIME_SPEND,
    CUSTOMER_TYPE
  FROM formula_1b82
), dim_customers AS (
  SELECT
    *
  FROM projection_66e2
)
SELECT
  *
FROM dim_customers