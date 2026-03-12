import sqlalchemy

import dagster as dg
from census_processing.defs.resources import PostGISResource


@dg.asset(
    key=["census", "2020", "linked"],
    deps=[
        ["census", "2020", "ent"],
        ["census", "2020", "mun"],
        ["census", "2020", "loc"],
        ["census", "2020", "ageb"],
        ["census", "2020", "mza"],
        ["metropoli_2020"],
    ],
    group_name="census_2020",
)
def link_tables(postgis_resource: PostGISResource) -> dg.MaterializeResult:
    level_order = ["mza", "ageb", "loc", "mun", "ent"]

    for i in range(5):
        for j in range(i + 1, 5):
            fk_table = f"census_2020_{level_order[i]}"
            pk_table = f"census_2020_{level_order[j]}"
            col = f"CVE_{level_order[j].upper()}"
            constraint_name = f"fk_census_2020_{level_order[i]}_{level_order[j]}"

            with postgis_resource.connect() as conn:
                conn.execute(
                    sqlalchemy.text(
                        f"ALTER TABLE {fk_table} DROP CONSTRAINT IF EXISTS {constraint_name}"
                    )
                )
                conn.execute(
                    sqlalchemy.text(
                        f'ALTER TABLE {fk_table} ADD CONSTRAINT {constraint_name} FOREIGN KEY ("{col}") REFERENCES {pk_table} ("CVEGEO")'
                    )
                )
                conn.commit()

    with postgis_resource.connect() as conn:
        conn.execute(
            sqlalchemy.text(
                'ALTER TABLE census_2020_mun ADD CONSTRAINT fk_census_2020_mun_met FOREIGN KEY ("CVE_MET") REFERENCES metropoli_2020 ("CVE_MET")'
            )
        )
        conn.commit()
    return dg.MaterializeResult(["census", "2020", "linked"])
