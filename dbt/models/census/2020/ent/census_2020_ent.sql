{{
    config(
        materialized="table",
        alias="census_2020_ent",
        indexes=spatial_index(),
    )
}}
select *
from {{ source("census_staging", "census_2020_ent_prepared") }}
