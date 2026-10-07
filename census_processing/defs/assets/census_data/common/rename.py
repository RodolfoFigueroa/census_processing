import json
from pathlib import Path

import pandas as pd

import dagster as dg
from census_processing.defs.resources import PathResource


def rename_columns_factory(year: int, prefix: str | None = None) -> dg.OpDefinition:
    @dg.op(
        name=f"rename_columns_{year}",
    )
    def _op(
        context: dg.OpExecutionContext, path_resource: PathResource, df: pd.DataFrame
    ) -> pd.DataFrame:
        config_path = Path(path_resource.config_path)
        name_map_path = config_path / "column_maps" / f"{year}.json"

        if not name_map_path.parent.exists():
            err = f"No column map directory found at {name_map_path.parent}"
            raise FileNotFoundError(err)

        if not name_map_path.exists():
            msg = (
                f"No column map found for year {year} at {name_map_path}, "
                "skipping renaming."
            )
            context.log.info(msg)
            return df

        with name_map_path.open(encoding="latin1") as f:
            column_map: dict = json.load(f)

        column_name_map = {
            f"{prefix or ''}{i + 1}": list(column_map.values())[i]
            for i in range(len(column_map))
        }
        wanted_cols = ["cvegeo"] + [
            key for key, value in column_name_map.items() if value is not None
        ]

        return df[wanted_cols].rename(columns=column_name_map)

    return _op


rename_columns_op_map = {
    1990: rename_columns_factory(1990, prefix=None),
    2000: rename_columns_factory(2000, prefix=None),
    2010: rename_columns_factory(2010, prefix=None),
    2020: rename_columns_factory(2020, prefix=None),
}
