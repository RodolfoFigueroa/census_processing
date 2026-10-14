{{
    config(
        materialized="table",
        alias="census_2000_ageb",
        indexes=spatial_index(
            [
                {"columns": ["cve_loc"], "type": "btree"},
                {"columns": ["cve_met"], "type": "btree"},
            ]
        ),
    )
}}

with parent_geographies as (
    {{
        add_parent_cvegeo(
            source("census_staging", "census_2000_ageb_prepared"),
            "ageb",
        )
    }}
)

{{
    assign_metropolitan_zone(
        "parent_geographies",
        ref("metropoli_2020"),
    )
}}
