with
    unit_ids as ({{ geo_unit_ids(2010, "mza") }}),
    parent_geographies as ({{ add_parent_cvegeo("unit_ids", "mza") }})
select cvegeo, cve_ageb from parent_geographies
