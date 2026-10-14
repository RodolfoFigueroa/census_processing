{# Compare complete rows, including duplicate counts and unrelated columns. #}
{% set cases = [
    ("ageb", "030178293912A", "cve_loc", "030178293"),
    ("loc", "030178293", "cve_mun", "03017"),
    ("mun", "03017", "cve_ent", "03"),
    ("ent", "03", none, none),
] %}

with
{% for level, cvegeo, parent_column, parent_id in cases %}
    input_{{ level }} as (
        select *
        from (values
            ('{{ cvegeo }}'::text, 7),
            ('{{ cvegeo }}'::text, 7),
            (null::text, 11)
        ) as fixture(cvegeo, payload)
    ),
    actual_{{ level }} as (
        {{ add_parent_cvegeo("input_" ~ level, level) }}
    ),
    expected_{{ level }} as (
        select cvegeo, payload
        {% if parent_column %}
            , case when cvegeo is null then null::text
                else '{{ parent_id }}'::text end as {{ parent_column }}
        {% endif %}
        from input_{{ level }}
    ),
    empty_input_{{ level }} as (
        select * from input_{{ level }} where false
    ),
    empty_actual_{{ level }} as (
        {{ add_parent_cvegeo("empty_input_" ~ level, level) }}
    ){% if not loop.last %},{% endif %}
{% endfor %}

{% for level, cvegeo, parent_column, parent_id in cases %}
    select '{{ level }}: unexpected rows or columns' as failure
    where exists (
        (select * from actual_{{ level }}
         except all select * from expected_{{ level }})
        union all
        (select * from expected_{{ level }}
         except all select * from actual_{{ level }})
    )
    union all
    select '{{ level }}: nonempty output for empty input' as failure
    where exists (select * from empty_actual_{{ level }})
    {% if not loop.last %}union all{% endif %}
{% endfor %}
