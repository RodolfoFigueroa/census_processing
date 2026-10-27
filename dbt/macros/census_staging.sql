{# The frozen census CSVs use exactly these three missing-value markers. #}
{% macro census_typed_value(expression, data_type) %}
    {% if data_type not in ["text", "int", "float"] %}
        {{ exceptions.raise_compiler_error("Unsupported census type: " ~ data_type) }}
    {% endif %}
    case
        when {{ expression }} in ('*', 'N/D', 'N/A') then null
        {% if data_type == "int" %}
            {# PostgreSQL numeric-to-integer casts round; reject fractions first.
               The error expression depends on the row to avoid constant folding. #}
            when {{ expression }}::numeric = trunc({{ expression }}::numeric)
                then {{ expression }}::numeric::integer
            else ('Non-integral census count: ' || {{ expression }})::integer
        {% elif data_type == "float" %}
            else {{ expression }}::double precision
        {% else %}
            else {{ expression }}::text
        {% endif %}
    end
{% endmacro %}

{% macro census_level(expression) %}
    case
        when {{ expression }} = repeat('0', 16) then 'national'
        when right({{ expression }}, 14) = repeat('0', 14) then 'ent'
        when right({{ expression }}, 11) = repeat('0', 11) then 'mun'
        when right({{ expression }}, 7) = repeat('0', 7) then 'loc'
        when right({{ expression }}, 3) = repeat('0', 3) then 'ageb'
        else 'mza'
    end
{% endmacro %}
