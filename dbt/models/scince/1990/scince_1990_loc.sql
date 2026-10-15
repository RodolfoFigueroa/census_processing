select *
from {{ source("census_staging", "scince_1990_prepared") }}
where cvegeo like '%0000' and cvegeo not like '%00000000'
