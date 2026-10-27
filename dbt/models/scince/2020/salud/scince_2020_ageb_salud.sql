select {{ scince_columns(2020, "salud") }}
from {{ ref("scince_2020_salud") }}
