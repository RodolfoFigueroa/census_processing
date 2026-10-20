{{
    config(
        indexes=spatial_index(),
    )
}}

select *
from {{ source("census_staging", "geometry_2020_loc_prepared") }}
