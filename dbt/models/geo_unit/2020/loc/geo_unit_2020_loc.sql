with
    unit_ids as ({{ geo_unit_ids(2020, "loc") }}),
    parent_geographies as ({{ add_parent_cvegeo("unit_ids", "loc") }})

select * from parent_geographies
