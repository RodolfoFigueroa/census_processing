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

{{
    add_parent_cvegeo(
        source("census_staging", "census_2010_loc_prepared"),
        "loc",
    )
}}
