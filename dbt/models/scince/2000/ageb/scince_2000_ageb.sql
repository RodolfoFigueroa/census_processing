{{
    config(
        materialized="table",
        alias="scince_2000_ageb",
    )
}}

select *
from {{ source("census_staging", "scince_2000_prepared") }}
where cvegeo not like '%0000'
