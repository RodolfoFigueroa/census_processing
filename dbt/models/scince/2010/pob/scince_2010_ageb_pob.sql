select {{ scince_columns(2010, "pob") }}
from {{ ref("scince_2010_pob") }}
