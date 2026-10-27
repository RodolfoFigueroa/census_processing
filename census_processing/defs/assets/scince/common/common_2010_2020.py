from collections.abc import Iterator
from typing import Literal

import pandas as pd

import dagster as dg
from census_processing.defs.assets.common import get_state_codes
from census_processing.readers.scince import concatenate_scince_theme


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
            df = concatenate_scince_theme(frame_maps, suffix)
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
                    "ingest",
                    str(year),
                    "scince",
                    suffix,
                ],
                metadata={
                    "schema": "ingest",
                    "table": f"scince_{year}_{suffix}",
                },
            )
            for suffix in table_suffixes
        },
        group_name=f"ingest/{year}/scince",
    )
    def _asset(scince_file_dep: None) -> dict[str, object]:
        state_codes = get_state_codes()
        frame_maps = state_codes.map(
            lambda state_code: process_op(state_code, scince_file_dep=scince_file_dep)
        )
        outputs = concat_frame_maps(frame_maps.collect())
        if len(table_suffixes) == 1:
            return {table_suffixes[0]: outputs}
        return outputs._asdict()

    return _asset
