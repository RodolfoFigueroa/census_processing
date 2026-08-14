SELECT
    ageb.*,
    candidate.cve_met
FROM staging.census_2010_ageb_prepared AS ageb
LEFT JOIN LATERAL (
    SELECT ranked.cve_met
    FROM (
        SELECT
            metropoli.cve_met,
            ST_Area(ST_Intersection(ageb.geometry, metropoli.geometry)) AS overlap_area
        FROM public.metropoli_2020 AS metropoli
        WHERE ST_Intersects(ageb.geometry, metropoli.geometry)
    ) AS ranked
    WHERE ranked.overlap_area > 0
    ORDER BY ranked.overlap_area DESC, ranked.cve_met ASC
    LIMIT 1
) AS candidate ON TRUE
