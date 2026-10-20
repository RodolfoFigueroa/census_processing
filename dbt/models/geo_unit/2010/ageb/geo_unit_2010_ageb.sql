with
    unit_ids as ({{ geo_unit_ids(2010, "ageb") }}),
    parent_geographies as ({{ add_parent_cvegeo("unit_ids", "ageb") }}),
    unit_geometries as (
        select units.*, geometry.geometry
        from parent_geographies as units
        left join {{ source("census_staging", "geometry_2010_ageb_prepared") }} as geometry
            on units.cvegeo = geometry.cvegeo
    ),
    metropolitan_assignments as (
        {{ assign_metropolitan_zone("unit_geometries", ref("metropoli_2020")) }}
    )
select cvegeo, cve_loc, cve_met from metropolitan_assignments
