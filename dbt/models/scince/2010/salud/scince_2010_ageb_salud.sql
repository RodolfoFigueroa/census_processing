select
    "cvegeo",
    "salud1",
    "salud2",
    "salud3",
    "salud4",
    "salud5",
    "salud6"
from {{ ref("scince_2010_salud") }}
