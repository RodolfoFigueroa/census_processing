select {{ scince_columns(2010, "salud") }}
from {{ ref("scince_2010_salud") }}
