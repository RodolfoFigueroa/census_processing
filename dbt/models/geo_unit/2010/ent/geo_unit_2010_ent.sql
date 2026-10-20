with
    unit_ids as ({{ geo_unit_ids(2010, "ent") }}),
    parent_geographies as ({{ add_parent_cvegeo("unit_ids", "ent") }})

select * from parent_geographies
