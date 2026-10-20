{{
    config(
        indexes=spatial_index(),
    )
}}

select *
from {{ source("census_staging", "geometry_2010_mun_prepared") }}
