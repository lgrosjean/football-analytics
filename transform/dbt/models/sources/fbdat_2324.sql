SELECT 
    *,
    '2324' AS season,
    rtrim(
        parse_path(filename, '/')[-1],
        '.parquet'
     ) as div,

FROM {{ source('fbdat', '2324') }}
