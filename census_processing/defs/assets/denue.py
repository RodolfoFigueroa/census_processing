import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from census_processing.defs.resources import PathResource


@dg.asset(
    key=["denue", "base"],
    ins={"df_mza": dg.AssetIn(["census", "2020", "mza"])},
    metadata={"table_name": "denue_05_2025"},
    io_manager_key="geodataframe_postgis_manager",
    group_name="denue",
)
def denue(path_resource: PathResource, df_mza: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    denue_path = Path(path_resource.data_path) / "raws" / "denue"

    df_list: list[gpd.GeoDataFrame] = []
    for path in denue_path.glob("*.zip"):
        with zipfile.ZipFile(path) as zf, tempfile.TemporaryDirectory() as tmpdir:
            zf.extractall(tmpdir)
            tmp_path = Path(tmpdir)
            df_list.append(
                gpd.read_file(tmp_path / "conjunto_de_datos").drop(
                    columns=[
                        "latitud",
                        "longitud",
                        "cve_ent",
                        "cve_mun",
                        "cve_loc",
                        "ageb",
                        "manzana",
                    ]
                )
            )

    return (
        pd.concat(df_list, ignore_index=True)
        .pipe(gpd.GeoDataFrame, geometry="geometry", crs="EPSG:4326")
        .to_crs("EPSG:6372")
    )
