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
                                  scince_relations, municipality_relations=[]) %}
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
    {% if length <= 5 %}
        {% for relation in municipality_relations %}
            union
            select {{ projected }} as cvegeo from {{ relation }}
            where {{ valid_geo_unit("cvegeo", "mun") }}
        {% endfor %}
    {% endif %}
{% endmacro %}

{% macro geo_unit_ids(year, level) %}
    {% if year == 2000 %}
        {% set width = {"ent": 2, "mun": 5, "loc": 9}[level] %}
        {% set prefix = "left(cvegeo, " ~ width ~ ")" %}
        select distinct {{ prefix }} as cvegeo
        from {{ source("census_staging", "scince_2000_prepared") }}
        where {{ valid_geo_unit(prefix, level) }}
        union
        select distinct {{ prefix }} as cvegeo
        from {{ source("census_staging", "geometry_2000_ageb_prepared") }}
        where {{ valid_geo_unit(prefix, level) }}
    {% elif year == 1990 %}
        {% if level not in ["ent", "mun"] %}
            {{ exceptions.raise_compiler_error("1990 identifier discovery supports ent and mun only") }}
        {% endif %}
        {% set width = 2 if level == "ent" else 5 %}
        {# SCINCE totals use zero-padded 13-character keys; retain their ancestors. #}
        with scince_identifiers as (
            select distinct left(cvegeo, {{ width }}) as cvegeo
            from {{ source("census_staging", "scince_1990_prepared") }}
        )
        {{ geo_unit_ids_from_sources(
            level,
            {"mun": source("census_staging", "geometry_1990_mun_prepared"),
             "ageb": source("census_staging", "geometry_1990_ageb_prepared")},
            source("census_staging", "census_1990"),
            {level: "scince_identifiers"}
        ) }}
    {% else %}
        {% set geometries = {} %}
        {% for source_level in ["ent", "mun", "loc", "ageb"] %}
            {% do geometries.update({source_level: source("census_staging", "geometry_" ~ year ~ "_" ~ source_level ~ "_prepared")}) %}
        {% endfor %}
        {% set scince = {} %}
        {% for source_level in ["ent", "mun", "loc", "ageb", "mza"] %}
            {% do scince.update({source_level: ref("scince_" ~ year ~ "_" ~ source_level ~ "_staging")}) %}
        {% endfor %}
        {% set municipalities = [] %}
        {% if year == 2020 and level in ["ent", "mun"] %}
            {% do municipalities.extend([
                source("census_staging", "metropoli_2020_prepared"),
                source("census_staging", "conapo_mun_prepared")
            ]) %}
        {% endif %}
        {{ geo_unit_ids_from_sources(
            level, geometries, ref("census_" ~ year),
            scince, municipalities
        ) }}
    {% endif %}
{% endmacro %}
