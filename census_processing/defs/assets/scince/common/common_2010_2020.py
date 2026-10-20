import os
from collections.abc import Iterator
from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd
from cfc_dagster_utils.utils import cast_all_columns_to_numeric
from dbfread import DBF

import dagster as dg
from census_processing.defs.assets.common import get_state_codes
from census_processing.defs.resources import ScinceColumnsToKeepResource


def cleanup_scince_columns_factory(year: Literal[2010, 2020]) -> dg.OpDefinition:
    @dg.op(name=f"cleanup_scince_columns_{year}")
    def _op(
        scince_columns_to_keep_resource: ScinceColumnsToKeepResource,
        frame_map: dict[str, pd.DataFrame],
    ) -> dict[str, pd.DataFrame]:
        cols_to_keep = set(getattr(scince_columns_to_keep_resource, f"columns_{year}"))
        out = {}
        for key, df in frame_map.items():
            df.columns = df.columns.astype(str).str.lower()
            ratio_cols = df.columns[df.columns.str.endswith("_r")]
            unwanted_cols = set(ratio_cols) - cols_to_keep
            out[key] = df.drop(columns=unwanted_cols)

        return out

    return _op


cleanup_scince_columns_op_map = {
    2010: cleanup_scince_columns_factory(2010),
    2020: cleanup_scince_columns_factory(2020),
}


def scince_2010_2020_factory(
    year: Literal[2010, 2020],
    process_op: dg.OpDefinition,
    table_suffixes: list[str],
) -> dg.AssetsDefinition:
    @dg.op(
        name=f"concat_frame_maps_{year}",
        out={
            suffix: dg.Out(
                dagster_type=pd.DataFrame,
                io_manager_key="postgres_manager",
                is_required=False,
            )
            for suffix in table_suffixes
        },
    )
    def concat_frame_maps(
        context: dg.OpExecutionContext,
        frame_maps: list[dict[str, pd.DataFrame]],
    ) -> Iterator[dg.Output[pd.DataFrame]]:
        for suffix in table_suffixes:
            if suffix not in context.selected_output_names:
                continue
            df = pd.concat(
                [frame_map[suffix] for frame_map in frame_maps], ignore_index=True
            )
            yield dg.Output(df, output_name=suffix)

    @dg.graph_multi_asset(
        name=f"scince_{year}_ageb",
        can_subset=True,
        ins={
            "scince_file_dep": dg.AssetIn(
                key=["input", str(year), "scince"], dagster_type=dg.Nothing
            )
        },
        outs={
            suffix: dg.AssetOut(
                key=[
                    "staging",
                    str(year),
                    "scince",
                    suffix,
                ],
                metadata={
                    "schema": "staging",
                    "table": f"scince_{year}_ageb_{suffix}_prepared",
                },
            )
            for suffix in table_suffixes
        },
        group_name=f"staging/{year}/scince",
    )
    def _asset(scince_file_dep: None) -> dict[str, object]:
        state_codes = get_state_codes()
        frame_maps = state_codes.map(
            lambda state_code: process_op(state_code, scince_file_dep=scince_file_dep)
        )
        outputs = concat_frame_maps(
            frame_maps.map(cleanup_scince_columns_op_map[year]).collect()
        )
        if len(table_suffixes) == 1:
            return {table_suffixes[0]: outputs}
        return outputs._asdict()

    return _asset


def process_dbf(path: os.PathLike, *, encoding: str | None = None) -> pd.DataFrame:
    dbf = DBF(path, encoding=encoding)

    return (
        pd.DataFrame(iter(dbf))
        .rename(columns=str.lower)
        .pipe(
            lambda df: cast_all_columns_to_numeric(
                df, ignore=["cvegeo", "grado_marg"], prefer_integer=True
            )
        )
    )


def read_extracted_tables(
    main_path: os.PathLike,
    supplementary_table_template: str,
    supplementary_table_labels_map: dict[str, str],
) -> dict[str, pd.DataFrame]:
    out = {}

    out["pob"] = (
        gpd.read_file(main_path)
        .drop(columns=["geometry", "OID"])
        .rename(columns=str.lower)
        .pipe(
            lambda df: cast_all_columns_to_numeric(
                df, ignore=["cvegeo"], prefer_integer=True
            )
        )
    )

    for short_label, file_suffix in supplementary_table_labels_map.items():
        out[short_label] = process_dbf(
            Path(supplementary_table_template.format_map({"file_suffix": file_suffix})),
            encoding="latin-1",
        )

    return out
