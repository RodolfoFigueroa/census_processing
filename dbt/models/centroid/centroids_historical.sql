{{
    config(
        indexes=spatial_index(),
    )
}}

select *
from {{ source("census_staging", "centroids_historical_prepared") }}
