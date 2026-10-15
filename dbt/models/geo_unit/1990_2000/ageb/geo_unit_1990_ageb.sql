{{
    geo_unit_ageb(
        geometry_relation=source("census_staging", "geometry_1990_ageb_prepared"),
        scince_relation=source("census_staging", "scince_1990_prepared"),
        metropolitan_relation=ref("metropoli_2020"),
    )
}}
