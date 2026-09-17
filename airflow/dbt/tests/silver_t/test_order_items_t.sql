SELECT
    order_item_id,
    'cleaning: order_item_id is null' AS failure_reason
FROM {{ ref('order_items_t') }}
WHERE order_item_id IS NULL

UNION ALL

SELECT
    order_item_id,
    'cleaning: duplicate order_item_id' AS failure_reason
FROM (
    SELECT order_item_id, COUNT(*) AS n
    FROM {{ ref('order_items_t') }}
    GROUP BY order_item_id
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT
    order_item_id,
    'cleaning: quantity not positive' AS failure_reason
FROM {{ ref('order_items_t') }}
WHERE quantity IS NULL OR quantity <= 0

UNION ALL

SELECT
    order_item_id,
    'cleaning: unit_price negative' AS failure_reason
FROM {{ ref('order_items_t') }}
WHERE unit_price IS NULL OR unit_price < 0

UNION ALL

SELECT
    oi.order_item_id,
    'normalisation: order_id not found in orders_t' AS failure_reason
FROM {{ ref('order_items_t') }} AS oi
LEFT JOIN {{ ref('orders_t') }} AS o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL

UNION ALL

SELECT
    oi.order_item_id,
    'normalisation: product_id not found in products_t' AS failure_reason
FROM {{ ref('order_items_t') }} AS oi
LEFT JOIN {{ ref('products_t') }} AS p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL