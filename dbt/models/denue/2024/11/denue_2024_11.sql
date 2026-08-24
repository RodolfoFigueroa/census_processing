{{
    config(
        materialized="table",
        alias="denue_2024_11",
    )
}}

SELECT *
FROM {{ source("census_staging", "denue_2024_11_prepared") }}