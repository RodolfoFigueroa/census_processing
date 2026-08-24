{{
    config(
        materialized="table",
        alias="denue_2020_11",
    )
}}

SELECT *
FROM {{ source("census_staging", "denue_2020_11_prepared") }}