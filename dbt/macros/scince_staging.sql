{% macro scince_typed_value(expression, data_type) %}
    {% if data_type == "text" %}
        {{ expression }}::text
    {% elif data_type in ["int", "float"] %}
        {# DBF numeric padding, suppression stars, and N/D are not measured zeros. #}
        {% set normalized = "replace(nullif(nullif(btrim(btrim(btrim(" ~ expression ~ "), '*')), ''), 'N/D'), ',', '.')" %}
        {% if data_type == "int" %}
            case
                when {{ normalized }} is null then null
                when {{ normalized }}::numeric = trunc({{ normalized }}::numeric)
                    then {{ normalized }}::numeric::integer
                else ('Non-integral SCINCE count: ' || {{ expression }})::integer
            end
        {% else %}
            {{ normalized }}::double precision
        {% endif %}
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported SCINCE type: " ~ data_type) }}
    {% endif %}
{% endmacro %}
