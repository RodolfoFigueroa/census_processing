with
    unit_ids as ({{ geo_unit_ids(2010, "mun") }}),
    parent_geographies as ({{ add_parent_cvegeo("unit_ids", "mun") }})

select * from parent_geographies
