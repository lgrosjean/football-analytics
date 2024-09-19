SELECT 
    *,
    '2425' AS season
FROM {{ source('football_data', '2425') }}
