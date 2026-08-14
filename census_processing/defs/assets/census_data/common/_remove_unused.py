from typing import Literal

import pandas as pd

import dagster as dg


def remove_unused_columns_factory(
    level: Literal["ent", "mun", "loc", "other"],
) -> dg.OpDefinition:
    @dg.op(name=f"remove_unused_columns_{level}")
    def _op(df: pd.DataFrame) -> pd.DataFrame:
        out = df.drop(
            columns=[
                "entidad",
                "mun",
                "loc",
                "ageb",
                "mza",
            ],
            errors="ignore",
        )

        unwanted_names = {"nom_ent", "nom_mun", "nom_loc"} - {f"nom_{level.lower()}"}
        return out.drop(columns=unwanted_names, errors="ignore")

    return _op


other_remove_op = remove_unused_columns_factory("other")
remove_unused_op_map: dict[str, dg.OpDefinition] = {
    "ent": remove_unused_columns_factory("ent"),
    "mun": remove_unused_columns_factory("mun"),
    "loc": remove_unused_columns_factory("loc"),
    "ageb": other_remove_op,
    "mza": other_remove_op,
}
