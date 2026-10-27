select {{ scince_columns(2010, "eco") }}
from {{ ref("scince_2010_eco") }}
