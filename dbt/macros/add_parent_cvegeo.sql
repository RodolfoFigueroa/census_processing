{# Input relations must contain text cvegeo and omit the derived parent column. #}
{% macro add_parent_cvegeo(relation, level) %}
    {% set parents = {
        "ageb": ("cve_loc", 9),
        "loc": ("cve_mun", 5),
        "mun": ("cve_ent", 2),
    } %}
    {% if level not in parents and level != "ent" %}
        {{ exceptions.raise_compiler_error(
            "add_parent_cvegeo: unsupported level '" ~ level
            ~ "'. Expected ageb, loc, mun, or ent."
        ) }}
    {% endif %}

    select
        census.*
        {% if level in parents %}
            , left(census.cvegeo, {{ parents[level][1] }}) as {{ parents[level][0] }}
        {% endif %}
    from {{ relation }} as census
{% endmacro %}
