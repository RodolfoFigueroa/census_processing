import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from census_processing.defs.assets.scince.common.common_2010_2020 import (
    scince_2010_2020_factory,
)
from census_processing.defs.resources import PathResource


@dg.op(
    ins={
        "state_code": dg.In(dagster_type=str),
        "scince_file_dep": dg.In(dagster_type=dg.Nothing),
    }
)
def process_state_code_scince_2010(
    path_resource: PathResource, state_code: str
) -> pd.DataFrame:
    in_path = Path(path_resource.in_path)
    fpath = in_path / "2010" / "scince" / f"{state_code}_SCINCE_zip.zip"
    with zipfile.ZipFile(fpath) as zf, tempfile.TemporaryDirectory() as tmpdir:
        zf.extractall(tmpdir)
        data_path = Path(tmpdir) / state_code

        return gpd.read_file(data_path / "ageb_urb.shp").drop(
            columns=["geometry", "OID"]
        )


scince_2010 = scince_2010_2020_factory(
    year=2010, process_state_code_op=process_state_code_scince_2010
)
