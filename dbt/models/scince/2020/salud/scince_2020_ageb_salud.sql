select
    "cvegeo",
    "salud1",
    "salud2",
    "salud3",
    "salud4",
    "salud5",
    "salud6",
    "salud7",
    "salud8",
    "salud9",
    "salud10"
from {{ ref("scince_2020_salud") }}
