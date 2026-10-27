select
    {{ scince_typed_value('"cvegeo"', "text") }} as "cvegeo",
    {{ scince_typed_value('"grado_marg"', "text") }} as "grado_marg",
    _source_file,
    _source_member
from {{ source("scince_ingest", "scince_2010_des") }}
