from collections.abc import Iterator

import pandas as pd
from cfc_dagster_utils.resources import PostgresResource

import dagster as dg


@dg.op(ins={"metropoli_2020": dg.In(dagster_type=dg.Nothing)}, out=dg.DynamicOut())
def get_met_zones(
    postgres_resource: PostgresResource,
) -> Iterator[dg.DynamicOutput[str]]:
    with postgres_resource.connect() as conn:
        df = pd.read_sql(
            """
            SELECT DISTINCT cve_met FROM metropoli_2020
            WHERE tipo_met IN ('Zona metropolitana', 'Metrópoli municipal')
            """,
            conn,
        )
        for elem in df["cve_met"]:
            yield dg.DynamicOutput(elem, mapping_key=str(elem).replace(".", "_"))
