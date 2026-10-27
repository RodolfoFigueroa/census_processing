select
    {{ scince_typed_value('"cvegeo"', "text") }} as "cvegeo",
    {{ scince_typed_value('"scony1"', "int") }} as "scony1",
    {{ scince_typed_value('"scony2"', "int") }} as "scony2",
    {{ scince_typed_value('"scony3"', "int") }} as "scony3",
    {{ scince_typed_value('"scony4"', "int") }} as "scony4",
    {{ scince_typed_value('"scony5"', "int") }} as "scony5",
    {{ scince_typed_value('"scony6"', "int") }} as "scony6",
    {{ scince_typed_value('"scony7"', "int") }} as "scony7",
    {{ scince_typed_value('"scony8"', "int") }} as "scony8",
    {{ scince_typed_value('"scony9"', "int") }} as "scony9",
    {{ scince_typed_value('"scony10"', "int") }} as "scony10",
    {{ scince_typed_value('"scony11"', "int") }} as "scony11",
    {{ scince_typed_value('"scony12"', "int") }} as "scony12",
    _source_file,
    _source_member
from {{ source("scince_ingest", "scince_2010_scony") }}
