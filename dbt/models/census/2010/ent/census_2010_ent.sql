select {{ census_columns(2010, cvegeo_length=2, keep_name="nom_ent") }}
from {{ ref("stg_census__2010") }}
where census_level = 'ent'
