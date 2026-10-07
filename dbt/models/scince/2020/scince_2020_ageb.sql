{{
    config(
        materialized="table",
        alias="scince_2020_ageb",
    )
}}

select *
from {{ source("census_staging", "scince_2020_ageb_prepared") }}
