import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from census_processing.defs.assets.common import (
    census_2010_2020_factory,
    merged_factory,
)
from census_processing.defs.resources import PathResource


@dg.op(name="geometry_2020_ageb")
def geometry_2020_ageb(path_resource: PathResource) -> gpd.GeoDataFrame:
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

            df_geoms.append(gpd.read_file(extracted_path / f"{i:02d}a.shp"))

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


census_2020_ageb = census_2010_2020_factory(
    year=2020,
    zip_template="ageb_mza_urbana_{i:02d}_cpv2020_csv.zip",
    inner_dir_template="ageb_mza_urbana_{i:02d}_cpv2020",
    csv_template="conjunto_de_datos_ageb_urbana_{i:02d}_cpv2020.csv",
)

ageb_2020 = merged_factory(
    census_op=census_2020_ageb,
    geometry_op=geometry_2020_ageb,
    year=2020,
)
