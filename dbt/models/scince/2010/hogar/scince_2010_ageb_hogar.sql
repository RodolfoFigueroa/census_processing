select {{ scince_columns(2010, "hogar") }}
from {{ ref("scince_2010_hogar") }}
