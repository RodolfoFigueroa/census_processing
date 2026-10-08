import subprocess
import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from census_processing.defs.assets.scince.common.scince_2010_2020 import (
    scince_2010_2020_factory,
)
from census_processing.defs.resources import PathResource


@dg.op(
    ins={
        "state_code": dg.In(dagster_type=str),
        "scince_file_dep": dg.In(dagster_type=dg.Nothing),
    }
)
def process_state_code(path_resource: PathResource, state_code: str) -> pd.DataFrame:
    in_path = Path(path_resource.in_path)
    fpath = in_path / "2020" / "scince" / f"SCINCE2020_DATOS_{state_code}.zip"

    with (
        zipfile.ZipFile(fpath) as zf,
        tempfile.TemporaryDirectory() as tmpdir_1,
        tempfile.TemporaryDirectory() as tmpdir_2,
    ):
        zf.extractall(tmpdir_1)
        exe_path = next(Path(tmpdir_1).glob("**/*.exe"))
        subprocess.run(  # noqa: S603
            ["/usr/bin/innoextract", str(exe_path), "-d", tmpdir_2],
            check=True,
            stdout=subprocess.DEVNULL,
        )

        data_path = next((Path(tmpdir_2) / "app").glob(f"{state_code}*"))
        main_path = data_path / "cartografia" / "ageb_urb.shp"

        return gpd.read_file(main_path).drop(columns=["geometry", "OID"])


scince_2020 = scince_2010_2020_factory(
    year=2020, process_state_code_op=process_state_code
)
