import io
import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd

import dagster as dg
from census_processing.defs.resources import PathResource


@dg.asset(
    name="ageb",
    key_prefix=["geometry", "2000"],
    io_manager_key="geodataframe_manager",
    group_name="geometry_2000",
)
def geometry_2000_ageb(path_resource: PathResource) -> gpd.GeoDataFrame:
    raw_path = Path(path_resource.data_path) / "raws"

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

    return (
        df_geom.assign(CVEGEO=lambda df: df["CLVAGB"].str.replace("-", ""))
        .drop(columns=["CLVAGB", "OID_1", "LAYAGB"])
        .set_index("CVEGEO")
        .to_crs("EPSG:6372")
    )


@dg.asset(
    name="loc",
    ins={"geometry_2000_ageb": dg.AssetIn(["geometry", "2000", "ageb"])},
    key_prefix=["geometry", "2000"],
    io_manager_key="geodataframe_manager",
    group_name="geometry_2000",
)
def geometry_2000_loc(geometry_2000_ageb: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return geometry_2000_ageb.assign(CVEGEO=lambda df: df["CVEGEO"].str[:9]).dissolve(
        by="CVEGEO",
    )
