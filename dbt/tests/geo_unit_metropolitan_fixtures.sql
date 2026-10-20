with
    zones as (
        select 'A'::text as cve_met, st_makeenvelope(0, 0, 2, 2, 6372) as geometry
        union all
        select 'B', st_makeenvelope(2, 0, 4, 2, 6372)
    ),
    agebs as (
        select 'greatest'::text as cvegeo, st_makeenvelope(1, 0, 4, 2, 6372) as geometry
        union all select 'tie', st_makeenvelope(1, 0, 3, 2, 6372)
        union all select 'boundary', st_makeenvelope(4, 0, 5, 2, 6372)
        union all select 'outside', st_makeenvelope(6, 0, 7, 2, 6372)
        union all select 'missing', null::geometry
    ),
    spatial_actual as ({{ assign_metropolitan_zone("agebs", "zones") }}),
    spatial_expected as (
        select * from (values
            ('greatest'::text, 'B'::text), ('tie', 'A'),
            ('boundary', null), ('outside', null), ('missing', null)
        ) as fixture(cvegeo, cve_met)
    ),
    municipalities as (
        select * from (values ('01001'::text), ('01002')) as fixture(cvegeo)
    ),
    mapping as (
        select * from (values ('01001'::text, 'A'::text), ('01001', 'A')) as fixture(cvegeo, cve_met)
    ),
    municipal_actual as ({{ assign_metropolitan_municipality("municipalities", "mapping") }}),
    conflicting_mapping as (
        select * from (values
            ('01001'::text, 'A'::text), ('01001', 'B'),
            ('01002', 'A'), ('01002', 'A'),
            ('01003', null), ('01003', 'A')
        ) as fixture(cvegeo, cve_met)
    ),
    conflicts as ({{ metropolitan_mapping_conflicts("conflicting_mapping") }}),
    expected_conflicts as (
        select * from (values ('01001'::text), ('01003')) as fixture(cvegeo)
    ),
    municipal_expected as (
        select * from (values ('01001'::text, 'A'::text), ('01002', null)) as fixture(cvegeo, cve_met)
    )
select 'incorrect spatial assignment' as failure
where exists (
    (select cvegeo, cve_met from spatial_actual except all select * from spatial_expected)
    union all
    (select * from spatial_expected except all select cvegeo, cve_met from spatial_actual)
)
union all
select 'incorrect municipality assignment' as failure
where exists (
    (select * from municipal_actual except all select * from municipal_expected)
    union all
    (select * from municipal_expected except all select * from municipal_actual)
)
union all
select 'incorrect conflict detection' as failure
where exists (
    (select * from conflicts except all select * from expected_conflicts)
    union all
    (select * from expected_conflicts except all select * from conflicts)
)
