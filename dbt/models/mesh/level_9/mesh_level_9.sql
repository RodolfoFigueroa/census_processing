{{
    config(
        materialized="table",
        alias="mesh_level_9",
        indexes=spatial_index(),
    )
}}

select *
from {{ source("census_staging", "mesh_level_9_prepared") }}
