from pathlib import Path

import pandas as pd

import dagster as dg
from census_processing.defs.assets.scince.common.common_2010_2020 import (
    scince_2010_2020_factory,
)
from census_processing.defs.resources import PathResource
from census_processing.readers.scince import SCINCE_TABLES, read_scince_archive


@dg.op(
    ins={
        "state_code": dg.In(dagster_type=str),
        "scince_file_dep": dg.In(dagster_type=dg.Nothing),
    }
)
def process_scince_ageb_2010(
    path_resource: PathResource, state_code: str
) -> dict[str, pd.DataFrame]:
    return read_scince_archive(
        Path(path_resource.in_path) / "2010" / "scince", 2010, state_code
    )


scince_2010_ageb_pop = scince_2010_2020_factory(
    year=2010,
    process_op=process_scince_ageb_2010,
    table_suffixes=list(SCINCE_TABLES[2010]),
)
