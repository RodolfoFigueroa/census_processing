{{
    config(
        materialized="table",
        alias="census_2010_mun",
        indexes=spatial_index([
            {"columns": ["cve_ent"], "type": "btree"}
        ])
    )
}}

SELECT *
FROM {{ source("census_staging", "census_2010_mun_prepared") }}