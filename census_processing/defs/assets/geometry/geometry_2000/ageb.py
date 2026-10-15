import io
import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd

import dagster as dg
from census_processing.defs.resources import PathResource


@dg.asset(
    key=["staging", "geometry", "2000", "ageb"],
    deps=[["input", "2000", "geometry", "ageb"]],
    io_manager_key="postgres_manager",
    metadata={"schema": "staging", "table": "geometry_2000_ageb_prepared"},
    group_name="staging/2000/geometry",
)
def geometry_2000_ageb(path_resource: PathResource) -> gpd.GeoDataFrame:
    raw_path = Path(path_resource.in_path)

    with (
        zipfile.ZipFile(raw_path / "2000" / "702825292843_s.zip") as f,
        f.open("mgau2000.zip") as subzip,
    ):
        subzip_data = io.BytesIO(subzip.read())
        with (
            zipfile.ZipFile(subzip_data) as f_nested,
            tempfile.TemporaryDirectory() as tmpdir,
        ):
            f_nested.extractall(tmpdir)
            df_geom = gpd.read_file(tmpdir)

    df_geom.columns = df_geom.columns.str.lower()

    return df_geom.assign(cvegeo=lambda df: df["clvagb"].str.replace("-", ""))[
        ["cvegeo", "geometry"]
    ].to_crs("EPSG:6372")
