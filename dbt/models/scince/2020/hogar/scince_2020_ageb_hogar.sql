select {{ scince_columns(2020, "hogar") }}
from {{ ref("scince_2020_hogar") }}
