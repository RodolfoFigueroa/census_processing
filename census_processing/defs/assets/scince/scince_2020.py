import json
import subprocess
import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from cfc_dagster_utils.utils import cast_all_columns_to_numeric

import dagster as dg
from census_processing.defs.resources import PathResource


@dg.asset(
    key=["staging", "2020", "scince", "ageb"],
    deps=[dg.AssetKey(["input", "2020", "scince"])],
    io_manager_key="postgres_manager",
    metadata={"schema": "staging", "table": "scince_2020_ageb_prepared"},
    group_name="staging_scince_2020",
)
def scince_2020_ageb(path_resource: PathResource) -> pd.DataFrame:
    in_path = Path(path_resource.in_path)

    config_path = Path(path_resource.config_path)
    cols_to_keep_path = config_path / "scince_cols_to_keep" / "2020.json"
    with cols_to_keep_path.open("r") as f:
        cols_to_keep = set(json.load(f).keys())

    out: list[pd.DataFrame] = []
    for i in range(1, 33):
        state_code = str(i).zfill(2)
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

            gdf = gpd.read_file(main_path).drop(columns=["geometry", "OID"])
            gdf.columns = gdf.columns.astype(str).str.lower()

            ratio_cols = gdf.columns[gdf.columns.str.endswith("_r")]
            unwanted_cols = set(ratio_cols) - cols_to_keep
            gdf = gdf.drop(columns=unwanted_cols)

            out.append(gdf)
    return (
        pd.concat(out)
        .sort_values("cvegeo")
        .reset_index(drop=True)
        .pipe(
            lambda df: cast_all_columns_to_numeric(
                df, ignore=["cvegeo"], prefer_integer=True
            )
        )
    )
