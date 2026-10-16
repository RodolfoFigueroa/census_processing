{{
    config(
        indexes=spatial_index(),
    )
}}

select *
from {{ source("census_staging", "metropoli_2020_prepared") }}
