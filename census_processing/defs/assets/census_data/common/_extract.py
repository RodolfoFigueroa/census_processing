from typing import Literal

import pandas as pd

import dagster as dg


def extract_census_level_factory(
    level: Literal["ent", "mun", "loc", "ageb", "mza"],
) -> dg.OpDefinition:
    @dg.op(name=f"extract_census_{level}")
    def _op(df: pd.DataFrame) -> pd.DataFrame:
        if level == "ageb":
            df = df.query("mza == '000'").assign(
                cvegeo=lambda df: df["cvegeo"].str[:-3]
            )
        elif level == "loc":
            df = df.query("mza == '000' and ageb == '0000'").assign(
                cvegeo=lambda df: df["cvegeo"].str[:-7]
            )
        elif level == "mun":
            df = df.query("mza == '000' and ageb == '0000' and loc == '0000'").assign(
                cvegeo=lambda df: df["cvegeo"].str[:-11]
            )
        elif level == "ent":
            df = df.query(
                "mza == '000' and ageb == '0000' and loc == '0000' and mun == '000'"
            ).assign(cvegeo=lambda df: df["cvegeo"].str[:2])

        return df

    return _op


extract_op_map = {
    "ent": extract_census_level_factory("ent"),
    "mun": extract_census_level_factory("mun"),
    "loc": extract_census_level_factory("loc"),
    "ageb": extract_census_level_factory("ageb"),
}
