{{
    config(
        materialized="table",
        alias="denue_2024_11",
        indexes=spatial_index(),
    )
}}

SELECT *
FROM {{ source("census_staging", "denue_2024_11_prepared") }}