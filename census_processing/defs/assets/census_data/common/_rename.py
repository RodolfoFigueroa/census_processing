import json
from pathlib import Path

import pandas as pd

import dagster as dg


def rename_columns_factory(year: int) -> dg.OpDefinition:
    @dg.op(
        name=f"rename_columns_{year}",
    )
    def _op(context: dg.OpExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
        name_map_path = (
            Path(__file__).parent.parent.parent.parent.parent
            / "config"
            / "column_maps"
            / f"{year}.json"
        )

        if not name_map_path.exists():
            msg = (
                f"No column map found for year {year} at {name_map_path}, "
                "skipping renaming."
            )
            context.log.warning(msg)
            return df

        with name_map_path.open(encoding="latin1") as f:
            column_map: dict = json.load(f)

        column_name_map = {
            i + 1: list(column_map.values())[i] for i in range(len(column_map))
        }
        wanted_cols = ["cvegeo"] + [
            key for key, value in column_name_map.items() if value is not None
        ]

        return df[wanted_cols].rename(columns=column_name_map)

    return _op


rename_columns_op_map = {
    year: rename_columns_factory(year) for year in (1990, 2000, 2010, 2020)
}
