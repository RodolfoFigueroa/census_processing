from pathlib import Path

import pandas as pd

import dagster as dg
from census_processing.defs.resources import PathResource
from census_processing.readers.scince import load_scince


def scince_2010_2020_factory(year: int, level: str) -> dg.AssetsDefinition:
    @dg.asset(
        key=["ingest", str(year), "scince", level],
        deps=[["input", str(year), "scince"]],
        io_manager_key="postgres_manager",
        metadata={"schema": "ingest", "table": f"scince_{year}_{level}"},
        group_name=f"ingest/{year}/scince",
    )
    def _asset(path_resource: PathResource) -> pd.DataFrame:
        return load_scince(
            Path(path_resource.in_path) / str(year) / "scince", year, level
        )

    return _asset
