{% for year in [2010, 2020] %}
    {% for level, parent, length in [("ageb", "loc", 9), ("loc", "mun", 5), ("mun", "ent", 2)] %}
        select '{{ year }}_{{ level }}'::text as model, child.cvegeo
        from {{ ref("geo_unit_" ~ year ~ "_" ~ level) }} as child
        left join {{ ref("geo_unit_" ~ year ~ "_" ~ parent) }} as parent
            on child.cve_{{ parent }} = parent.cvegeo
        where parent.cvegeo is null
            or child.cve_{{ parent }} is distinct from left(child.cvegeo, {{ length }})
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
    {% if not loop.last %}union all{% endif %}
{% endfor %}
