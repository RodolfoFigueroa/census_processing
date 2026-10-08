{{
    config(
        materialized="table",
        alias="scince_2010_ageb",
    )
}}

select *
from {{ source("census_staging", "scince_2010_ageb_prepared") }}
