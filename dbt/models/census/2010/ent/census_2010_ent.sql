{{
    config(
        materialized="table",
        alias="census_2010_ent",
    )
}}

SELECT *
FROM {{ source("census_staging", "census_2010_ent_prepared") }}