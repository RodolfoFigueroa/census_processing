{# Public projections follow the checked-in contract, never a live relation. #}
{% macro census_columns(year, cvegeo_length, keep_name=none) %}
    {% if keep_name is not none and keep_name not in ["nom_ent", "nom_mun", "nom_loc"] %}
        {{ exceptions.raise_compiler_error(
            "census_columns: keep_name must be nom_ent, nom_mun, nom_loc, or none."
        ) }}
    {% endif %}
    {% for column in var("census_" ~ year ~ "_columns") %}
        {% if column.name == "cvegeo" %}
            left(cvegeo, {{ cvegeo_length }}) as cvegeo
        {% else %}
            {{ adapter.quote(column.name) }}
        {% endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}
    {% if keep_name is not none %}, {{ adapter.quote(keep_name) }}{% endif %}
{% endmacro %}
