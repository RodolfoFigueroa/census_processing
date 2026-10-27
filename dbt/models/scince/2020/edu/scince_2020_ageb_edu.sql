select {{ scince_columns(2020, "edu") }}
from {{ ref("scince_2020_edu") }}
