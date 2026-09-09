{{
    config(
        materialized="table",
        alias="census_2020_loc",
        indexes=spatial_index([{"columns": ["cve_mun"], "type": "btree"}]),
    )
}}

select *

from {{ source("census_staging", "census_2020_loc_prepared") }}
