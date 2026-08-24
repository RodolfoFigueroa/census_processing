{{
    config(
        materialized="table",
        alias="denue_2025_05",
    )
}}

SELECT *
FROM {{ source("census_staging", "denue_2025_05_prepared") }}