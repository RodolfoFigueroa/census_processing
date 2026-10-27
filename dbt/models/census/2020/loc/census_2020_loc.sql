select {{ census_columns(2020, cvegeo_length=9, keep_name="nom_loc") }}
from {{ ref("census_2020") }}
where census_level = 'loc'
