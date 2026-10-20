{{
    config(
        indexes=spatial_index(),
    )
}}

select *
from {{ source("census_staging", "geometry_2020_ent_prepared") }}
