{{
    config(
        materialized="table",
        alias="denue_2022_11",
        indexes=spatial_index(),
    )
}}

SELECT *
FROM {{ source("census_staging", "denue_2022_11_prepared") }}