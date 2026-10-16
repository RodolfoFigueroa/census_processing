{{
    config(
        indexes=spatial_index(),
    )
}}

SELECT *
FROM {{ source("census_staging", "denue_2020_11_prepared") }}