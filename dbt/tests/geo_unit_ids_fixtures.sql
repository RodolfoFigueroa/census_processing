{% set cases = {
    "ent": ["01", "02", "03", "04", "05", "06", "07", "08"],
    "mun": ["01001", "02002", "03003", "04004", "05005", "06006", "07007", "08008"],
    "loc": ["010010001", "020020002", "030030003", "040040004", "050050005", "060060006"],
    "ageb": ["010010001001A", "020020002002B", "030030003003C", "040040004004D"]
} %}
with
    census as (
        select * from (values
            ('010010001001A000'::text),
            ('010010001001A000'),
            ('0500500050000000'),
            ('0700700000000000'),
            ('0800000000000000'),
            ('090090009009E001') -- Block rows are not AGEB summary rows.
        ) as fixture(cvegeo)
    ),
    geometry as (select '020020002002B'::text as cvegeo),
    locality as (select '060060006'::text as cvegeo),
    population as (
        select * from (values ('030030003003C'::text), ('9909900990000')) as fixture(cvegeo)
    ),
    supplementary as (select '040040004004D'::text as cvegeo),
    metropolitan as (select '08008'::text as cvegeo),
    empty_input as (select null::text as cvegeo where false),
    {% for level, identifiers in cases.items() %}
        actual_{{ level }} as (
            {{ geo_unit_ids_from_sources(level, {"ageb": "geometry", "loc": "locality"},
                "census", ["population", "supplementary"], "metropolitan") }}
        ),
        empty_{{ level }} as (
            {{ geo_unit_ids_from_sources(level, {"ageb": "empty_input"},
                "empty_input", ["empty_input"], "empty_input") }}
        ),
        expected_{{ level }} as (
            select * from (values
                {% for identifier in identifiers %}
                    ('{{ identifier }}'::text){% if not loop.last %},{% endif %}
                {% endfor %}
            ) as fixture(cvegeo)
        ){% if not loop.last %},{% endif %}
    {% endfor %}
{% for level in cases %}
    select '{{ level }}: incorrect identifiers or duplicates' as failure
    where exists (
        (select * from actual_{{ level }} except all select * from expected_{{ level }})
        union all
        (select * from expected_{{ level }} except all select * from actual_{{ level }})
    )
    union all
    select '{{ level }}: nonempty result for empty sources' as failure
    where exists (select * from empty_{{ level }})
    {% if not loop.last %}union all{% endif %}
{% endfor %}
