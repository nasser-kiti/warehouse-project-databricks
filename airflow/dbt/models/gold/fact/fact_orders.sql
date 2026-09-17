WITH orders AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        store_id,
        employee_id,
        customer_id,
        order_timestamp,
        total_amount,
        quantity,
        unit_price,
        line_amount
    FROM {{ ref('obt_b') }}
),

/*For each dimension: rank candidate versions so the one whose valid-range
 actually covers order_timestamp wins (priority 0). If none do — i.e. the
order predates the earliest captured snapshot for that entity — fall back
to the earliest available version instead (priority 1), since no earlier
version was ever recorded to match against.*/
customer_resolved AS (
    SELECT order_item_id, dbt_scd_id AS customer_scd_id
    FROM (
        SELECT
            orders.order_item_id,
            dim.dbt_scd_id,
            ROW_NUMBER() OVER (
                PARTITION BY orders.order_item_id
                ORDER BY
                    CASE
                        WHEN orders.order_timestamp >= dim.dbt_valid_from
                         AND orders.order_timestamp <  dim.dbt_valid_to
                        THEN 0 ELSE 1
                    END,
                    dim.dbt_valid_from ASC
            ) AS rn
        FROM orders
        LEFT JOIN {{ ref('dim_customers') }} AS dim
            ON orders.customer_id = dim.customer_id
    )
    WHERE rn = 1
),

product_resolved AS (
    SELECT order_item_id, dbt_scd_id AS product_scd_id
    FROM (
        SELECT
            orders.order_item_id,
            dim.dbt_scd_id,
            ROW_NUMBER() OVER (
                PARTITION BY orders.order_item_id
                ORDER BY
                    CASE
                        WHEN orders.order_timestamp >= dim.dbt_valid_from
                         AND orders.order_timestamp <  dim.dbt_valid_to
                        THEN 0 ELSE 1
                    END,
                    dim.dbt_valid_from ASC
            ) AS rn
        FROM orders
        LEFT JOIN {{ ref('dim_products') }} AS dim
            ON orders.product_id = dim.product_id
    )
    WHERE rn = 1
),

store_resolved AS (
    SELECT order_item_id, dbt_scd_id AS store_scd_id
    FROM (
        SELECT
            orders.order_item_id,
            dim.dbt_scd_id,
            ROW_NUMBER() OVER (
                PARTITION BY orders.order_item_id
                ORDER BY
                    CASE
                        WHEN orders.order_timestamp >= dim.dbt_valid_from
                         AND orders.order_timestamp <  dim.dbt_valid_to
                        THEN 0 ELSE 1
                    END,
                    dim.dbt_valid_from ASC
            ) AS rn
        FROM orders
        LEFT JOIN {{ ref('dim_stores') }} AS dim
            ON orders.store_id = dim.store_id
    )
    WHERE rn = 1
),

employee_resolved AS (
    SELECT order_item_id, dbt_scd_id AS employee_scd_id
    FROM (
        SELECT
            orders.order_item_id,
            dim.dbt_scd_id,
            ROW_NUMBER() OVER (
                PARTITION BY orders.order_item_id
                ORDER BY
                    CASE
                        WHEN orders.order_timestamp >= dim.dbt_valid_from
                         AND orders.order_timestamp <  dim.dbt_valid_to
                        THEN 0 ELSE 1
                    END,
                    dim.dbt_valid_from ASC
            ) AS rn
        FROM orders
        LEFT JOIN {{ ref('dim_employees') }} AS dim
            ON orders.employee_id = dim.employee_id
    )
    WHERE rn = 1
)

SELECT
    -- KEYS
    orders.order_id,
    orders.order_item_id,
    orders.product_id,
    orders.store_id,
    orders.employee_id,
    orders.customer_id,

    -- POINT-IN-TIME DIMENSION REFERENCES
    customer_resolved.customer_scd_id,
    product_resolved.product_scd_id,
    store_resolved.store_scd_id,
    employee_resolved.employee_scd_id,

    -- EVENT TIME
    orders.order_timestamp,

    -- MEASURES
    orders.total_amount,
    orders.quantity,
    orders.unit_price,
    orders.line_amount

FROM orders
LEFT JOIN customer_resolved ON orders.order_item_id = customer_resolved.order_item_id
LEFT JOIN product_resolved  ON orders.order_item_id = product_resolved.order_item_id
LEFT JOIN store_resolved    ON orders.order_item_id = store_resolved.order_item_id
LEFT JOIN employee_resolved ON orders.order_item_id = employee_resolved.order_item_id