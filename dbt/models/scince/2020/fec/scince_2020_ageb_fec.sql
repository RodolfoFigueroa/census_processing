select
    "cvegeo",
    "fec1_r",
    "fec2_r",
    "fec3_r"
from {{ ref("scince_2020_fec") }}
