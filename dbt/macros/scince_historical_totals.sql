{% macro scince_historical_totals(year, level) %}
    {% if year not in [1990, 2000] %}
        {{ exceptions.raise_compiler_error("Unsupported historical SCINCE year: " ~ year) }}
    {% endif %}
    {% set widths = {"ent": 2, "mun": 5, "loc": 9} %}
    {% if level not in widths %}
        {{ exceptions.raise_compiler_error("Unsupported historical SCINCE total level: " ~ level) }}
    {% endif %}
    {% set width = widths[level] %}
    {% set prefix = "left(cvegeo, " ~ width ~ ")" %}

    select
        {{ prefix }} as cvegeo,
        {% for indicator in range(1, 72 if year == 1990 else 171) %}
            {{ adapter.quote("pob" ~ indicator) }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ source("census_staging", "scince_" ~ year ~ "_prepared") }}
    where
        cvegeo ~ '^[0-9]{13}$'
        and right(cvegeo, {{ 13 - width }}) = '{{ "0" * (13 - width) }}'
        and {{ valid_geo_unit(prefix, level) }}
{% endmacro %}
