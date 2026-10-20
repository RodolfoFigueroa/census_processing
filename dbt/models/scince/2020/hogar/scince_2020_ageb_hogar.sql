select * from {{ source("census_staging", "scince_2020_ageb_hogar_prepared") }}
