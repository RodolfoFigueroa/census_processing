with unit_ids as ({{ geo_unit_ids(2000, "loc") }})
{{ add_parent_cvegeo("unit_ids", "loc") }}
