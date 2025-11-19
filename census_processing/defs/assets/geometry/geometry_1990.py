import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd

import dagster as dg
from census_processing.defs.resources import PathResource


@dg.op(
    name="geometry_1990_ageb",
)
def geometry_1990_ageb(path_resource: PathResource) -> gpd.GeoDataFrame:
    raw_path = Path(path_resource.data_path) / "raws"
    with (
        zipfile.ZipFile(raw_path / "1990" / "AGEBs 90_TecMonty_aj.zip") as zf,
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        zf.extractall(tmpdir)
        df = gpd.read_file(Path(tmpdir) / "AGEBs 90_TecMonty_aj")

    return (
        df.assign(
            CVEGEO=lambda df: df["CVE_ENT"].astype(str).str.zfill(2)
            + df["CVE_MUN"].astype(str).str.zfill(3)
            + df["CVE_LOC"].astype(str).str.zfill(4)
            + df["CVE_AGEB"].astype(str).str.zfill(4),
        )
        .drop(columns=["CVE_ENT", "CVE_MUN", "CVE_LOC", "CVE_AGEB", "OBJECTID"])
        .to_crs("EPSG:6372")
    )
