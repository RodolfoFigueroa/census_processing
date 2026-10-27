select {{ scince_columns(2020, "eco") }}
from {{ ref("scince_2020_eco") }}
