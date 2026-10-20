{% macro assign_metropolitan_municipality(municipality_relation, metropolitan_relation) %}
    select municipalities.*,
        (
            select distinct mapping.cve_met
            from {{ metropolitan_relation }} as mapping
            where mapping.cvegeo = municipalities.cvegeo
        ) as cve_met
    from {{ municipality_relation }} as municipalities
    {# A scalar subquery rejects conflicting assignments instead of choosing one. #}
{% endmacro %}

{% macro metropolitan_mapping_conflicts(relation) %}
    select cvegeo
    from (select distinct cvegeo, cve_met from {{ relation }}) as mapping
    group by cvegeo
    having count(*) > 1
{% endmacro %}
