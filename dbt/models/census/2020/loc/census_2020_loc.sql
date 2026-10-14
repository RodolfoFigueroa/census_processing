{{
    config(
        materialized="table",
        alias="census_2020_loc",
        indexes=spatial_index([{"columns": ["cve_mun"], "type": "btree"}]),
    )
}}

{{
    add_parent_cvegeo(
        source("census_staging", "census_2020_loc_prepared"),
        "loc",
    )
}}
