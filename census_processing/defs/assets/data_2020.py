import tempfile
import zipfile
from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd

import dagster as dg
from census_processing.defs.assets.common import (
    full_census_2010_2020_factory,
    merged_factory,
)
from census_processing.defs.resources import PathResource


def geometry_2020_factory(
    level: Literal["ent", "mun", "loc", "ageb", "mza"],
) -> dg.OpDefinition:
    @dg.op(name=f"geometry_2020_{level}")
    def _op(path_resource: PathResource) -> gpd.GeoDataFrame:
        raw_path = Path(path_resource.data_path) / "raws"

        df_geoms: list[gpd.GeoDataFrame] = []
        for i in range(1, 33):
            root_dir = raw_path / "2020" / "geom"
            compressed_path = next(root_dir.glob(f"{i:02d}_*"))

            with (
                tempfile.TemporaryDirectory() as tmpdir,
                zipfile.ZipFile(compressed_path) as zf,
            ):
                zf.extractall(tmpdir)
                extracted_path = Path(tmpdir) / compressed_path.stem

                suffix_map = {
                    "ent": "ent",
                    "mun": "mun",
                    "loc": "l",
                    "ageb": "a",
                    "mza": "m",
                }
                df_geoms.append(
                    gpd.read_file(extracted_path / f"{i:02d}{suffix_map[level]}.shp")
                )

        crs = df_geoms[0].crs
        if crs is None:
            err = "CRS is None for geometry of state 0"
            raise ValueError(err)

        return (
            pd.concat(df_geoms, ignore_index=True)[["CVEGEO", "geometry"]]
            .pipe(
                gpd.GeoDataFrame,
                geometry="geometry",
                crs=crs,
            )
            .to_crs("EPSG:6372")
        )

    return _op


census_2020_ageb = full_census_2010_2020_factory(
    year=2020,
    zip_template="ageb_mza_urbana_{i:02d}_cpv2020_csv.zip",
    inner_dir_template="ageb_mza_urbana_{i:02d}_cpv2020",
    csv_template="conjunto_de_datos_ageb_urbana_{i:02d}_cpv2020.csv",
    level="ageb",
)

geometry_2020_ageb = geometry_2020_factory(level="ageb")

ageb_2020 = merged_factory(
    census_op=census_2020_ageb, geometry_op=geometry_2020_ageb, year=2020, level="ageb"
)


census_2020_mza = full_census_2010_2020_factory(
    year=2020,
    zip_template="ageb_mza_urbana_{i:02d}_cpv2020_csv.zip",
    inner_dir_template="ageb_mza_urbana_{i:02d}_cpv2020",
    csv_template="conjunto_de_datos_ageb_urbana_{i:02d}_cpv2020.csv",
    level="mza",
)

geometry_2020_mza = geometry_2020_factory(level="mza")

mza_2020 = merged_factory(
    census_op=census_2020_mza, geometry_op=geometry_2020_mza, year=2020, level="mza"
)
