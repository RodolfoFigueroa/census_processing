from pathlib import Path

import pandas as pd

import dagster as dg
from census_processing.defs.resources import PathResource
from census_processing.readers.census import load_census


def census_ingest_factory(year: int) -> dg.AssetsDefinition:
    @dg.asset(
        key=["ingest", "census", str(year)],
        deps=[["input", str(year), "demography"]],
        io_manager_key="postgres_manager",
        metadata={"schema": "ingest", "table": f"census_{year}"},
        group_name="ingest/census",
    )
    def _asset(path_resource: PathResource) -> pd.DataFrame:
        return load_census(Path(path_resource.in_path) / str(year) / "census", year)

    return _asset


census_2010 = census_ingest_factory(2010)
census_2020 = census_ingest_factory(2020)
