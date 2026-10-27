select {{ census_columns(2010, cvegeo_length=9, keep_name="nom_loc") }}
from {{ ref("stg_census__2010") }}
where census_level = 'loc'
