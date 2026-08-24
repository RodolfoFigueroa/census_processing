{{
    config(
        materialized="table",
        alias="denue_2021_11",
    )
}}

SELECT *
FROM {{ source("census_staging", "denue_2021_11_prepared") }}