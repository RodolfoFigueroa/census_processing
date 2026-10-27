select {{ scince_columns(2020, "fec") }}
from {{ ref("scince_2020_fec") }}
