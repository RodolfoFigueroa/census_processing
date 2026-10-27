select
    {{ scince_typed_value('"cvegeo"', "text") }} as "cvegeo",
    {{ scince_typed_value('"salud1"', "int") }} as "salud1",
    {{ scince_typed_value('"salud2"', "int") }} as "salud2",
    {{ scince_typed_value('"salud3"', "int") }} as "salud3",
    {{ scince_typed_value('"salud4"', "int") }} as "salud4",
    {{ scince_typed_value('"salud5"', "int") }} as "salud5",
    {{ scince_typed_value('"salud6"', "int") }} as "salud6",
    {{ scince_typed_value('"salud7"', "int") }} as "salud7",
    {{ scince_typed_value('"salud8"', "int") }} as "salud8",
    {{ scince_typed_value('"salud9"', "int") }} as "salud9",
    {{ scince_typed_value('"salud10"', "int") }} as "salud10",
    _source_file,
    _source_member
from {{ source("scince_ingest", "scince_2020_salud") }}
