{{
    config(
        materialized="table",
        alias="denue_2022_11",
    )
}}

SELECT *
FROM {{ source("census_staging", "denue_2022_11_prepared") }}