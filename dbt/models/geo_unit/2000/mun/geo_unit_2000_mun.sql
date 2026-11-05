with unit_ids as ({{ geo_unit_ids(2000, "mun") }})
{{ add_parent_cvegeo("unit_ids", "mun") }}
