from pathlib import Path

import pandas as pd

import dagster as dg
from census_processing.defs.resources import PathResource
from census_processing.readers.conapo import CONAPO_ARCHIVE, read_conapo_archive


@dg.asset(
    key=["staging", "conapo", "mun"],
    deps=[["input", "conapo"]],
    io_manager_key="postgres_manager",
    metadata={"schema": "staging", "table": "conapo_mun_prepared"},
    group_name="staging/conapo",
)
def conapo_mun_prepared(path_resource: PathResource) -> pd.DataFrame:
    return read_conapo_archive(Path(path_resource.in_path) / CONAPO_ARCHIVE)
