with
    unit_ids as ({{ geo_unit_ids(2020, "mun") }}),
    parent_geographies as ({{ add_parent_cvegeo("unit_ids", "mun") }})

{{ assign_metropolitan_municipality("parent_geographies", source("census_staging", "metropoli_2020_prepared")) }}
