{% set staging = source("census_staging", "census_2010_prepared") %}

select {{ census_columns(staging, cvegeo_length=9, keep_name="nom_loc") }}
from {{ staging }}
where
    cvegeo like '%0000000'
    and cvegeo not like '%00000000000'
    and cvegeo not like '%00000000000000'
