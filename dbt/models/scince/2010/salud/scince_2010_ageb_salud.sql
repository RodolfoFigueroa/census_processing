select * from {{ source("census_staging", "scince_2010_ageb_salud_prepared") }}
