from collections.abc import Sequence
from typing import Literal

import pandas as pd
from cfc_dagster_utils.utils import cast_all_columns_to_numeric

import dagster as dg


@dg.op(out=dg.Out(io_manager_key="postgres_manager"))
def concat_and_clean_scince_dataframes(df_list: Sequence[pd.DataFrame]) -> pd.DataFrame:
    out: pd.DataFrame = (
        pd.concat(df_list)
        .replace(["*", "N.D.", "N/D"], pd.NA)
        .sort_index()
        .pipe(lambda df: cast_all_columns_to_numeric(df, prefer_integer=True))
        .reset_index(names="cvegeo")
    )
    out.columns = out.columns.astype(str).str.lower()
    return out.add_prefix("pob").rename(columns={"pobcvegeo": "cvegeo"})


def scince_1990_2000_factory(
    year: Literal[1990, 2000], load_scince_op: dg.OpDefinition
) -> dg.AssetsDefinition:
    @dg.graph_asset(
        key=["staging", str(year), "scince"],
        ins={
            "scince_file_dep": dg.AssetIn(
                key=["input", str(year), "scince"], dagster_type=dg.Nothing
            )
        },
        metadata={"schema": "staging", "table": f"scince_{year}_prepared"},
        group_name=f"staging/{year}",
    )
    def _asset(scince_file_dep: None) -> pd.DataFrame:
        return concat_and_clean_scince_dataframes(load_scince_op(scince_file_dep))

    return _asset
