select
    {{ scince_typed_value('"cvegeo"', "text") }} as "cvegeo",
    {{ scince_typed_value('"relig1"', "int") }} as "relig1",
    {{ scince_typed_value('"relig2"', "int") }} as "relig2",
    {{ scince_typed_value('"relig3"', "int") }} as "relig3",
    {{ scince_typed_value('"relig4"', "int") }} as "relig4",
    _source_file,
    _source_member
from {{ source("scince_ingest", "scince_2020_relig") }}
