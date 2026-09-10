{{
    config(
        materialized="table",
        alias="denue_2021_11",
        indexes=spatial_index(),
    )
}}

SELECT *
FROM {{ source("census_staging", "denue_2021_11_prepared") }}