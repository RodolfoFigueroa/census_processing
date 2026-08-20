{{
    config(
        materialized="table",
        alias="census_2010_ageb",
        indexes=spatial_index([
            {"columns": ["cve_ent"], "type": "btree"},    
            {"columns": ["cve_mun"], "type": "btree"},
            {"columns": ["cve_met"], "type": "btree"}
        ])
    )
}}

SELECT
    ageb.*,
    candidate.cve_met
FROM {{ source("census_staging", "census_2010_ageb_prepared") }} AS ageb
LEFT JOIN LATERAL (
    SELECT ranked.cve_met
    FROM (
        SELECT
            metropoli.cve_met,
            ST_Area(ST_Intersection(ageb.geometry, metropoli.geometry)) AS overlap_area
        FROM {{ source("published_inputs", "metropoli_2020") }} AS metropoli
        WHERE ST_Intersects(ageb.geometry, metropoli.geometry)
    ) AS ranked
    WHERE ranked.overlap_area > 0
    ORDER BY ranked.overlap_area DESC, ranked.cve_met ASC
    LIMIT 1
) AS candidate ON TRUE
