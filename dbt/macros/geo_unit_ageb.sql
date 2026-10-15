{# Geometry has one row per cvegeo; SCINCE also contains aggregate IDs ending in 0000. #}
{% macro geo_unit_ageb(geometry_relation, scince_relation, metropolitan_relation) %}
    with
        unit_ids as (
            select cvegeo
            from {{ geometry_relation }}
            union
            select cvegeo
            from {{ scince_relation }}
            where cvegeo not like '%0000'
        ),
        parent_geographies as ({{ add_parent_cvegeo("unit_ids", "ageb") }}),
        unit_geometries as (
            select units.*, geometry.geometry
            from parent_geographies as units
            left join {{ geometry_relation }} as geometry
                on units.cvegeo = geometry.cvegeo
        ),
        metropolitan_assignments as (
            {{ assign_metropolitan_zone("unit_geometries", metropolitan_relation) }}
        )

    select cvegeo, cve_loc, cve_met
    from metropolitan_assignments
{% endmacro %}
