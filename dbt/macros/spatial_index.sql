{% macro spatial_index(extra_indexes=none, geometry_column="geometry") %}
    {% set extra_indexes = extra_indexes or [] %}

    {{ return(
        [
            {
                "columns": [geometry_column],
                "type": "gist"
            }
        ] + extra_indexes
    ) }}
{% endmacro %}