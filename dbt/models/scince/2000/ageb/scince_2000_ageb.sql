select *
from {{ source("census_staging", "scince_2000_prepared") }}
where cvegeo not like '%0000'
