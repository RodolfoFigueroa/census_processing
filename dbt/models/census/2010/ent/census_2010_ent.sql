{{
    config(
        materialized="table",
        alias="census_2010_ent",
        indexes=spatial_index(),
    )
}}
select *
from {{ source("census_staging", "census_2010_ent_prepared") }}
