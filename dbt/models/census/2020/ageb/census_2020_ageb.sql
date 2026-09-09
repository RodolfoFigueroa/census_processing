{{
    config(
        materialized="table",
        alias="census_2020_ageb",
        indexes=spatial_index([{"columns": ["cve_loc"], "type": "btree"}]),
    )
}}

select *

from {{ source("census_staging", "census_2020_ageb_prepared") }}
