SELECT
    customer_id,
    'cleaning: customer_id is null' AS failure_reason
FROM {{ ref('customers_t') }}
WHERE customer_id IS NULL

UNION ALL

SELECT
    customer_id,
    'cleaning: duplicate customer_id' AS failure_reason
FROM (
    SELECT customer_id, COUNT(*) AS n
    FROM {{ ref('customers_t') }}
    GROUP BY customer_id
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT
    customer_id,
    'standardisation: email missing @' AS failure_reason
FROM {{ ref('customers_t') }}
WHERE email IS NOT NULL AND email NOT LIKE '%@%'

UNION ALL

SELECT
    customer_id,
    'standardisation: is_active not in (Y, N)' AS failure_reason
FROM {{ ref('customers_t') }}
WHERE is_active NOT IN ('Y', 'N')

UNION ALL

SELECT
    customer_id,
    'enrichment: processed_at is null' AS failure_reason
FROM {{ ref('customers_t') }}
WHERE processed_at IS NULL