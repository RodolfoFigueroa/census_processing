{{
    config(
        materialized="table",
        alias="denue_2023_11",
        indexes=spatial_index(),
    )
}}

SELECT *
FROM {{ source("census_staging", "denue_2023_11_prepared") }}