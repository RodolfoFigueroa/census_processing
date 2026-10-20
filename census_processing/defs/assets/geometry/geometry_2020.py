import tempfile
import zipfile
from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd

import dagster as dg
from census_processing.defs.resources import PathResource

SUFFIX_MAP = {
    "ent": ["ent"],
    "mun": ["mun"],
    "loc": ["l"],
    "ageb": ["a"],
    "mza": ["m"],
}


def geometry_2020_factory(
    level: Literal["ent", "mun", "loc", "ageb", "mza"],
) -> dg.AssetsDefinition:
    @dg.asset(
        key=["staging", "2020", "geometry", f"{level}"],
        deps=[["input", "2020", "geometry"]],
        io_manager_key="postgres_manager",
        metadata={"schema": "staging", "table": f"geometry_2020_{level}_prepared"},
        group_name="staging/2020/geometry",
    )
    def _asset(path_resource: PathResource) -> gpd.GeoDataFrame:
        raw_path = Path(path_resource.in_path)

        df_geoms_list: list[gpd.GeoDataFrame] = []
        for i in range(1, 33):
            root_dir = raw_path / "2020" / "geom"
            compressed_path = next(root_dir.glob(f"{i:02d}_*"))

            with (
                tempfile.TemporaryDirectory() as tmpdir,
                zipfile.ZipFile(compressed_path) as zf,
            ):
                zf.extractall(tmpdir)
                extracted_path = Path(tmpdir) / compressed_path.stem

                for suffix in SUFFIX_MAP[level]:
                    df_read = gpd.read_file(extracted_path / f"{i:02d}{suffix}.shp")
                    if level == "mza":
                        df_read = df_read.query("AMBITO == 'Urbana'")
                    df_geoms_list.append(df_read)

        crs = df_geoms_list[0].crs
        if crs is None:
            err = "CRS is None for geometry of state 0"
            raise ValueError(err)

        return (
            pd.concat(df_geoms_list, ignore_index=True)[["CVEGEO", "geometry"]]
            .rename(columns={"CVEGEO": "cvegeo"})
            .pipe(lambda df: gpd.GeoDataFrame(df, geometry="geometry", crs=crs))
            .to_crs("EPSG:6372")
        )

    return _asset


geometry_2020_assets = [
    geometry_2020_factory(level) for level in ("ent", "mun", "loc", "ageb", "mza")
]
