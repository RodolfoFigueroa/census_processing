import io
import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd

import dagster as dg
from census_processing.defs.resources import PathResource


@dg.asset(
    name="2000",
    key_prefix="geometry",
    io_manager_key="geodataframe_manager",
    group_name="geometry",
)
def geometry_2000(path_resource: PathResource) -> gpd.GeoDataFrame:
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
