from typing import Literal

import pandas as pd
from cfc_dagster_utils.utils import cast_all_columns_to_numeric

import dagster as dg
from census_processing.defs.assets.common import get_state_codes
from census_processing.defs.resources import ScinceColumnsToKeepResource


def cleanup_scince_columns_factory(year: Literal[2010, 2020]) -> dg.OpDefinition:
    @dg.op(name=f"cleanup_scince_columns_{year}")
    def _op(
        scince_columns_to_keep_resource: ScinceColumnsToKeepResource, df: pd.DataFrame
    ) -> pd.DataFrame:
        df.columns = df.columns.astype(str).str.lower()

        ratio_cols = df.columns[df.columns.str.endswith("_r")]

        cols_to_keep = set(getattr(scince_columns_to_keep_resource, f"columns_{year}"))
        unwanted_cols = set(ratio_cols) - cols_to_keep

        return df.drop(columns=unwanted_cols)

    return _op


cleanup_scince_columns_op_map = {
    2010: cleanup_scince_columns_factory(2010),
    2020: cleanup_scince_columns_factory(2020),
}


@dg.op(out=dg.Out(io_manager_key="postgres_manager"))
def concat_scince_dfs(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    return (
        pd.concat(dfs)
        .sort_values("cvegeo")
        .reset_index(drop=True)
        .pipe(
            lambda df: cast_all_columns_to_numeric(
                df, ignore=["cvegeo"], prefer_integer=True
            )
        )
    )


def scince_2010_2020_factory(
    year: Literal[2010, 2020], process_state_code_op: dg.OpDefinition
) -> dg.AssetsDefinition:
    @dg.graph_asset(
        key=["staging", str(year), "scince", "ageb"],
        ins={"scince_file_dep": dg.AssetIn(key=["input", str(year), "scince"])},
        metadata={"schema": "staging", "table": f"scince_{year}_ageb_prepared"},
        group_name=f"staging/scince/{year}",
    )
    def _asset(scince_file_dep: None) -> pd.DataFrame:
        state_codes = get_state_codes()
        frames = state_codes.map(
            lambda state_code: process_state_code_op(
                state_code, scince_file_dep=scince_file_dep
            )
        )
        cleaned = frames.map(cleanup_scince_columns_op_map[year])
        return concat_scince_dfs(cleaned.collect())

    return _asset
