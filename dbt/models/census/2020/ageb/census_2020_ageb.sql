{{
    config(
        materialized="table",
        alias="census_2020_ageb",
        indexes=spatial_index([{"columns": ["cve_loc"], "type": "btree"}]),
    )
}}

{{
    add_parent_cvegeo(
        source("census_staging", "census_2020_ageb_prepared"),
        "ageb",
    )
}}
