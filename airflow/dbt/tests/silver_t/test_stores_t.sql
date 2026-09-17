SELECT
    store_id,
    'cleaning: store_id is null' AS failure_reason
FROM {{ ref('stores_t') }}
WHERE store_id IS NULL

UNION ALL

SELECT
    store_id,
    'cleaning: duplicate store_id' AS failure_reason
FROM (
    SELECT store_id, COUNT(*) AS n
    FROM {{ ref('stores_t') }}
    GROUP BY store_id
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT
    store_id,
    'standardisation: is_active not in (Y, N)' AS failure_reason
FROM {{ ref('stores_t') }}
WHERE is_active NOT IN ('Y', 'N')

UNION ALL

SELECT
    store_id,
    'enrichment: processed_at is null' AS failure_reason
FROM {{ ref('stores_t') }}
WHERE processed_at IS NULL