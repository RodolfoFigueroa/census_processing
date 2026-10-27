select {{ census_columns(2020, cvegeo_length=2, keep_name="nom_ent") }}
from {{ ref("stg_census__2020") }}
where census_level = 'ent'
