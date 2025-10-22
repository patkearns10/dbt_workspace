WITH orders AS (
  SELECT
    *
  FROM {{ ref('orders') }}
), c AS (
  SELECT
    *
  FROM {{ ref('customers') }}
), aggregate_1 AS (
  SELECT
    CUSTOMER_ID,
    COUNT(ORDER_ID) AS TOTAL_ORDERS
  FROM orders
  GROUP BY
    CUSTOMER_ID
), rename_2 AS (
  SELECT
    *
    RENAME (CUSTOMER_ID AS C_CUSTOMER_ID)
  FROM c
), rename_1 AS (
  SELECT
    CUSTOMER_ID AS COC_CUSTOMER_ID,
    TOTAL_ORDERS
  FROM aggregate_1
), join_1 AS (
  SELECT
    *
  FROM rename_2
  JOIN rename_1
    ON rename_2.C_CUSTOMER_ID = rename_1.COC_CUSTOMER_ID
), formula_1 AS (
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY TOTAL_ORDERS DESC) AS RANK
  FROM join_1
), rename_3 AS (
  SELECT
    C_CUSTOMER_ID AS CUSTOMER_ID,
    CUSTOMER_NAME,
    TOTAL_ORDERS,
    RANK
  FROM formula_1
), filter_1 AS (
  SELECT
    *
  FROM rename_3
  WHERE
    RANK <= 10
), order_1 AS (
  SELECT
    *
  FROM filter_1
  ORDER BY
    TOTAL_ORDERS DESC
), untitled_sql AS (
  SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    TOTAL_ORDERS
  FROM order_1
)
SELECT
  *
FROM untitled_sql