select {{ scince_columns(2010, "fec") }}
from {{ ref("scince_2010_fec") }}
