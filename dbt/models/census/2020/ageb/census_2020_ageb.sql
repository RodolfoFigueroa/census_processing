select {{ census_columns(2020, cvegeo_length=13) }}
from {{ ref("stg_census__2020") }}
where census_level = 'ageb'
