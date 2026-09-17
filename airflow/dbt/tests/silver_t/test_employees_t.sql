SELECT
    employee_id,
    'cleaning: employee_id is null' AS failure_reason
FROM {{ ref('employees_t') }}
WHERE employee_id IS NULL

UNION ALL

SELECT
    employee_id,
    'cleaning: duplicate employee_id' AS failure_reason
FROM (
    SELECT employee_id, COUNT(*) AS n
    FROM {{ ref('employees_t') }}
    GROUP BY employee_id
    HAVING COUNT(*) > 1
)

UNION ALL

SELECT
    employee_id,
    'cleaning: salary not positive' AS failure_reason
FROM {{ ref('employees_t') }}
WHERE salary IS NULL OR salary <= 0

UNION ALL

SELECT
    employee_id,
    'standardisation: is_active not in (Y, N)' AS failure_reason
FROM {{ ref('employees_t') }}
WHERE is_active NOT IN ('Y', 'N')

UNION ALL

SELECT
    e.employee_id,
    'normalisation: store_id not found in stores_t' AS failure_reason
FROM {{ ref('employees_t') }} AS e
LEFT JOIN {{ ref('stores_t') }} AS s ON e.store_id = s.store_id
WHERE e.store_id IS NOT NULL AND s.store_id IS NULL