{{
    config(
        materialized="table",
        alias="census_2020_mun",
        indexes=spatial_index([{"columns": ["cve_ent"], "type": "btree"}]),
    )
}}

select *

from {{ source("census_staging", "census_2020_mun_prepared") }}
