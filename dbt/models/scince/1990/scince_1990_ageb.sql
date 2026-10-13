{{
    config(
        materialized="table",
        alias="scince_1990_ageb",
    )
}}

select *
from {{ source("census_staging", "scince_1990_ageb_prepared") }}
