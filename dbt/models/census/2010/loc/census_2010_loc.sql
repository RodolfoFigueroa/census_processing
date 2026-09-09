{{
    config(
        materialized="table",
        alias="census_2010_loc",
        indexes=spatial_index(
            [
                {"columns": ["cve_mun"], "type": "btree"},
            ]
        ),
    )
}}

select *
from {{ source("census_staging", "census_2010_loc_prepared") }}
