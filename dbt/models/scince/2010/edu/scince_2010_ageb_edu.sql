select {{ scince_columns(2010, "edu") }}
from {{ ref("scince_2010_edu") }}
