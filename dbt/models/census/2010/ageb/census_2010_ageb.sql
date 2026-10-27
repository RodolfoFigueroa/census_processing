select {{ census_columns(2010, cvegeo_length=13) }}
from {{ ref("stg_census__2010") }}
where census_level = 'ageb'
