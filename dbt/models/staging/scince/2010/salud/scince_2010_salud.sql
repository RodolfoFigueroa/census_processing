select
    {{ scince_typed_value('"cvegeo"', "text") }} as "cvegeo",
    {{ scince_typed_value('"salud1"', "int") }} as "salud1",
    {{ scince_typed_value('"salud2"', "int") }} as "salud2",
    {{ scince_typed_value('"salud3"', "int") }} as "salud3",
    {{ scince_typed_value('"salud4"', "int") }} as "salud4",
    {{ scince_typed_value('"salud5"', "int") }} as "salud5",
    {{ scince_typed_value('"salud6"', "int") }} as "salud6",
    _source_file,
    _source_member
from {{ source("scince_ingest", "scince_2010_salud") }}
