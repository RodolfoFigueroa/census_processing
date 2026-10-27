select {{ census_columns(2010, cvegeo_length=13) }}
from {{ ref("census_2010") }}
where census_level = 'ageb'
