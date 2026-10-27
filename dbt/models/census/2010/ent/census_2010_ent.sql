{% set staging = source("census_staging", "census_2010_prepared") %}

select {{ census_columns(staging, cvegeo_length=2, keep_name="nom_ent") }}
from {{ staging }}
where cvegeo like '%00000000000000' and cvegeo != '0000000000000000'
