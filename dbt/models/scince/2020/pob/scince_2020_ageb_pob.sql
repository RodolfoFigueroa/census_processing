select {{ scince_columns(2020, "pob") }}
from {{ ref("scince_2020_pob") }}
