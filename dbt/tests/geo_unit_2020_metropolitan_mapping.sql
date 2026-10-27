with mapping as (
    select distinct cvegeo, cve_met
    from {{ source("census_staging", "metropoli_2020_prepared") }}
)
select 'membership mismatch' as failure, municipalities.cvegeo
from {{ ref("geo_unit_2020_mun") }} as municipalities
left join mapping using (cvegeo)
where municipalities.cve_met is distinct from mapping.cve_met
