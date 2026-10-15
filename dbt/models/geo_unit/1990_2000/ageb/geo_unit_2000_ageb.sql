{{
    geo_unit_ageb(
        geometry_relation=source("census_staging", "geometry_2000_ageb_prepared"),
        scince_relation=source("census_staging", "scince_2000_prepared"),
        metropolitan_relation=ref("metropoli_2020"),
    )
}}
