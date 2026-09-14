{{
    config(
        materialized='incremental',
        unique_key='employee_id'
    )
}}

SELECT
    *,
    current_timestamp() AS processed_at
FROM
    {{ source('walmart_databricks', 'employees') }}

-- Only process new or updated records for incremental runs
{% if is_incremental() %}
    WHERE updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1900-01-01') FROM {{ this }})
{% endif %}