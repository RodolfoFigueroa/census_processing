select
    cast(cvegeo as text) as cvegeo,
    cast(year as integer) as year,
    cast(pob_mit_mun as integer) as pob_mit_mun
from {{ source("census_staging", "conapo_mun_prepared") }}
