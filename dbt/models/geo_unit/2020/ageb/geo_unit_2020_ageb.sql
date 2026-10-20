with
    unit_ids as ({{ geo_unit_ids(2020, "ageb") }}),
    parent_geographies as ({{ add_parent_cvegeo("unit_ids", "ageb") }})

select * from parent_geographies
