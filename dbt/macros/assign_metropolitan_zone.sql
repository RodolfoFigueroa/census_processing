{% macro assign_metropolitan_zone(ageb_relation, metropoli_relation) %}
    select ageb.*, candidate.cve_met
    from {{ ageb_relation }} as ageb
    left join
        lateral(
            select ranked.cve_met
            from
                (
                    select
                        metropoli.cve_met,
                        st_area(
                            st_intersection(ageb.geometry, metropoli.geometry)
                        ) as overlap_area
                    from {{ metropoli_relation }} as metropoli
                    where st_intersects(ageb.geometry, metropoli.geometry)
                ) as ranked
            where ranked.overlap_area > 0
            order by ranked.overlap_area desc, ranked.cve_met asc
            limit 1
        ) as candidate
        on true
{% endmacro %}
