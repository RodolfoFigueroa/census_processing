from typing import Literal

import pandas as pd

import dagster as dg

LEVEL_MULTIPLIERS = {
    "ent": 14,
    "mun": 11,
    "loc": 7,
    "ageb": 3,
}


def extract_census_level_factory(
    level: Literal["ent", "mun", "loc", "ageb", "mza"],
) -> dg.OpDefinition:
    @dg.op(name=f"extract_census_{level}")
    def _op(df: pd.DataFrame) -> pd.DataFrame:
        s = LEVEL_MULTIPLIERS[level]
        return df.loc[lambda df: df["cvegeo"].str.endswith("0" * s)].assign(
            cvegeo=lambda df: df["cvegeo"].str[:-s]
        )

    return _op


extract_op_map = {
    "ent": extract_census_level_factory("ent"),
    "mun": extract_census_level_factory("mun"),
    "loc": extract_census_level_factory("loc"),
    "ageb": extract_census_level_factory("ageb"),
}
