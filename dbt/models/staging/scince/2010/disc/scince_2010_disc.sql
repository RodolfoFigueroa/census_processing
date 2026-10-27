select
    {{ scince_typed_value('"cvegeo"', "text") }} as "cvegeo",
    {{ scince_typed_value('"disc1"', "int") }} as "disc1",
    {{ scince_typed_value('"disc2"', "int") }} as "disc2",
    {{ scince_typed_value('"disc3"', "int") }} as "disc3",
    {{ scince_typed_value('"disc4"', "int") }} as "disc4",
    {{ scince_typed_value('"disc5"', "int") }} as "disc5",
    {{ scince_typed_value('"disc6"', "int") }} as "disc6",
    {{ scince_typed_value('"disc14"', "int") }} as "disc14",
    _source_file,
    _source_member
from {{ source("scince_ingest", "scince_2010_disc") }}
