{# Match the former pandas reader's null policy without inferring SQL types. #}
{% macro census_typed_value(expression, data_type) %}
    {% if data_type not in ["text", "int", "float"] %}
        {{ exceptions.raise_compiler_error("Unsupported census type: " ~ data_type) }}
    {% endif %}
    case
        when {{ expression }} in (
            {% for token in var("census_null_tokens") %}
                '{{ token | replace("'", "''") }}'{% if not loop.last %},{% endif %}
            {% endfor %}
        ) then null
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
