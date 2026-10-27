{# Identifier projection only: no statistics are aggregated or inferred. #}
{% macro valid_geo_unit(expression, level) %}
    {% set widths = {"ent": 2, "mun": 5, "loc": 9, "ageb": 13, "mza": 16} %}
    {% set patterns = {"ent": "[0-9]{2}", "mun": "[0-9]{5}", "loc": "[0-9]{9}",
                       "ageb": "[0-9]{9}[0-9A-Za-z]{4}", "mza": "[0-9]{9}[0-9A-Za-z]{4}[0-9]{3}"} %}
    {% if level not in widths %}
        {{ exceptions.raise_compiler_error("Unsupported geographic level: " ~ level) }}
    {% endif %}
    {{ expression }} ~ '^{{ patterns[level] }}$'
    {% for end, start, width in [(2, 1, 2), (5, 3, 3), (9, 6, 4), (13, 10, 4), (16, 14, 3)] %}
        {% if end <= widths[level] %}
            and substring({{ expression }} from {{ start }} for {{ width }}) <> '{{ "0" * width }}'
        {% endif %}
    {% endfor %}
{% endmacro %}

{# Only staging relations participate: final parents must not depend on children. #}
{% macro geo_unit_ids_from_sources(level, geometry_relations, census_relation,
                                  scince_relations, metropolitan_relation=none) %}
    {% set lengths = {"ent": 2, "mun": 5, "loc": 9, "ageb": 13, "mza": 16} %}
    {% set length = lengths[level] %}
    {% set projected = "left(cvegeo, " ~ length ~ ")" %}

    select distinct {{ projected }} as cvegeo
    from {{ census_relation }}
    where {{ valid_geo_unit(projected, level) }}

    {% for relations in [geometry_relations, scince_relations] %}
        {% for source_level, relation in relations.items() %}
            {% if lengths[source_level] >= length %}
                union
                select {{ projected }} as cvegeo from {{ relation }}
                where {{ valid_geo_unit("cvegeo", source_level) }}
            {% endif %}
        {% endfor %}
    {% endfor %}
    {% if metropolitan_relation is not none and length <= 5 %}
        union
        select {{ projected }} as cvegeo from {{ metropolitan_relation }}
        where {{ valid_geo_unit("cvegeo", "mun") }}
    {% endif %}
{% endmacro %}

{% macro geo_unit_ids(year, level) %}
    {% set geometries = {} %}
    {% for source_level in ["ent", "mun", "loc", "ageb"] %}
        {% do geometries.update({source_level: source("census_staging", "geometry_" ~ year ~ "_" ~ source_level ~ "_prepared")}) %}
    {% endfor %}
    {% set scince = {} %}
    {% for source_level in ["ent", "mun", "loc", "ageb", "mza"] %}
        {% do scince.update({source_level: ref("scince_" ~ year ~ "_" ~ source_level ~ "_staging")}) %}
    {% endfor %}
    {{ geo_unit_ids_from_sources(
        level, geometries, ref("census_" ~ year),
        scince, source("census_staging", "metropoli_2020_prepared") if year == 2020 else none
    ) }}
{% endmacro %}
