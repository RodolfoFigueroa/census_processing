import json
from pathlib import Path

import pandas as pd

import dagster as dg
from census_processing.defs.resources import PathResource


def add_derived_columns_factory(year: int) -> dg.OpDefinition:
    @dg.op(
        name=f"add_derived_columns_{year}",
    )
    def _op(path_resource: PathResource, df: pd.DataFrame) -> pd.DataFrame:
        config_path = Path(path_resource.config_path)

        with (config_path / "derived_cols" / f"{year}.json").open() as f:
            column_expr: dict[str, str] = json.load(f)

        return df.assign(**{col: df.eval(expr) for col, expr in column_expr.items()})  # ty:ignore[invalid-argument-type]

    return _op


add_derived_columns_op_map = {
    year: add_derived_columns_factory(year) for year in (1990, 2000, 2010, 2020)
}
