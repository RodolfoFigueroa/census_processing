select
    {{ scince_typed_value('"cvegeo"', "text") }} as "cvegeo",
    {{ scince_typed_value('"fec1_r"', "float") }} as "fec1_r",
    {{ scince_typed_value('"fec2_r"', "float") }} as "fec2_r",
    {{ scince_typed_value('"fec3_r"', "float") }} as "fec3_r",
    _source_file,
    _source_member
from {{ source("scince_ingest", "scince_2020_fec") }}
