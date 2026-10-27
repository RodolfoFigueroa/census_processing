select {{ census_columns(2020, cvegeo_length=13) }}
from {{ ref("census_2020") }}
where census_level = 'ageb'
