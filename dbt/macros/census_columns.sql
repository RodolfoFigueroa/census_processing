{# Trim cvegeo and keep all source columns except unrequested geographic names. #}
{% macro census_columns(relation, cvegeo_length, keep_name=none) %}
    {% set name_columns = ["nom_ent", "nom_mun", "nom_loc"] %}
    {% if keep_name is not none and keep_name not in name_columns %}
        {{ exceptions.raise_compiler_error(
            "census_columns: keep_name must be nom_ent, nom_mun, nom_loc, or none."
        ) }}
    {% endif %}

    {# Parsing must not query the database; compilation expands the column list. #}
    {% if not execute %}
        {{ return("*") }}
    {% endif %}

    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% if not columns %}
        {{ exceptions.raise_compiler_error(
            "census_columns: no columns found for " ~ relation
            ~ ". The staging relation must exist before compilation."
        ) }}
    {% endif %}
    {% if keep_name is not none and keep_name not in columns | map(attribute="name") | list %}
        {{ exceptions.raise_compiler_error(
            "census_columns: " ~ relation ~ " is missing " ~ keep_name ~ "."
        ) }}
    {% endif %}

    {% set selected = [] %}
    {% for column in columns %}
        {% if column.name == "cvegeo" %}
            {% do selected.append(
                "left(" ~ adapter.quote(column.name) ~ ", " ~ cvegeo_length ~ ") as "
                ~ adapter.quote(column.name)
            ) %}
        {% elif column.name not in name_columns or column.name == keep_name %}
            {% do selected.append(adapter.quote(column.name)) %}
        {% endif %}
    {% endfor %}
    {{ return(selected | join(", ")) }}
{% endmacro %}
