{{
    config(
        materialized="table",
        alias="census_2010_ageb",
        indexes=spatial_index(
            [
                {"columns": ["cve_loc"], "type": "btree"},
                {"columns": ["cve_met"], "type": "btree"},
            ]
        ),
    )
}}

select ageb.*, candidate.cve_met
from {{ source("census_staging", "census_2010_ageb_prepared") }} as ageb
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
                from {{ ref("metropoli_2020") }} as metropoli
                where st_intersects(ageb.geometry, metropoli.geometry)
            ) as ranked
        where ranked.overlap_area > 0
        order by ranked.overlap_area desc, ranked.cve_met asc
        limit 1
    ) as candidate
    on true
