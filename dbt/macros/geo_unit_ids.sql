{# Only staging relations participate: final parents must not depend on children. #}
{% macro geo_unit_ids_from_sources(level, geometry_relations, census_relation,
                                  scince_relations, metropolitan_relation=none) %}
    {% set lengths = {"ent": 2, "mun": 5, "loc": 9, "ageb": 13} %}
    {% set segments = {"ent": (1, 2), "mun": (3, 3), "loc": (6, 4), "ageb": (10, 4)} %}
    {% set length = lengths[level] %}
    {% set start, width = segments[level] %}

    select left(cvegeo, {{ length }}) as cvegeo
    from {{ census_relation }}
    where right(cvegeo, 3) = '000'
        and substring(cvegeo from {{ start }} for {{ width }}) <> '{{ "0" * width }}'

    {% for source_level, relation in geometry_relations.items() %}
        {% if lengths[source_level] >= length %}
            union
            select left(cvegeo, {{ length }}) as cvegeo from {{ relation }}
        {% endif %}
    {% endfor %}
    {% for relation in scince_relations %}
        union
        select left(cvegeo, {{ length }}) as cvegeo
        from {{ relation }}
        where right(cvegeo, 4) <> '0000'
    {% endfor %}
    {% if metropolitan_relation is not none and length <= 5 %}
        union
        select left(cvegeo, {{ length }}) as cvegeo from {{ metropolitan_relation }}
    {% endif %}
{% endmacro %}

{% macro geo_unit_ids(year, level) %}
    {% set geometries = {} %}
    {% for source_level in ["ent", "mun", "loc", "ageb"] %}
        {% do geometries.update({source_level: source("census_staging", "geometry_" ~ year ~ "_" ~ source_level ~ "_prepared")}) %}
    {% endfor %}
    {% set themes = ["disc", "eco", "edu", "fec", "hogar", "indi", "mig", "pob", "relig", "salud", "scony", "viv"] %}
    {% do themes.append("des" if year == 2010 else "mor") %}
    {% set scince = [] %}
    {% for theme in themes %}
        {% do scince.append(source("census_staging", "scince_" ~ year ~ "_ageb_" ~ theme ~ "_prepared")) %}
    {% endfor %}
    {{ geo_unit_ids_from_sources(
        level, geometries, ref("census_" ~ year),
        scince, source("census_staging", "metropoli_2020_prepared") if year == 2020 else none
    ) }}
{% endmacro %}
