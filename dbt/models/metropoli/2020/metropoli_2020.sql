{{
    config(
        indexes=spatial_index(),
    )
}}

select
    cve_met,
    min(nom_met) as nom_met,
    min(tipo_met) as tipo_met,
    st_union(geometry) as geometry
from {{ source("census_staging", "metropoli_2020_prepared") }}
where cve_met is not null
group by cve_met
