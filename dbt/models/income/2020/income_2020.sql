select * from {{ source("census_staging", "income_2020_prepared") }}
