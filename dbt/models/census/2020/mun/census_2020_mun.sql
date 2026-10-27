select {{ census_columns(2020, cvegeo_length=5, keep_name="nom_mun") }}
from {{ ref("census_2020") }}
where census_level = 'mun'
