{% set staging = source("census_staging", "census_2020_prepared") %}

select {{ census_columns(staging, cvegeo_length=13) }}
from {{ staging }}
where
    cvegeo like '%000'
    and cvegeo not like '%0000000'
    and cvegeo not like '%00000000000'
    and cvegeo not like '%00000000000000'
