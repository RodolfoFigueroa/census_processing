select {{ census_columns(2010, cvegeo_length=5, keep_name="nom_mun") }}
from {{ ref("stg_census__2010") }}
where census_level = 'mun'
