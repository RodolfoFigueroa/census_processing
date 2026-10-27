select
    {{ scince_typed_value('"cvegeo"', "text") }} as "cvegeo",
    {{ scince_typed_value('"mor1_r"', "float") }} as "mor1_r",
    _source_file,
    _source_member
from {{ source("scince_ingest", "scince_2010_mor") }}
