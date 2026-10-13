import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd

import dagster as dg
from census_processing.defs.resources import PathResource


@dg.asset(
    key=["staging", "1990", "census", "ageb"],
    deps=[["input", "1990", "geometry", "ageb"]],
    io_manager_key="postgres_manager",
    metadata={"schema": "staging", "table": "census_1990_ageb_prepared"},
    group_name="staging_census_1990",
)
def geometry_1990_ageb(path_resource: PathResource) -> gpd.GeoDataFrame:
    raw_path = Path(path_resource.in_path)
    with (
        zipfile.ZipFile(raw_path / "1990" / "AGEBs 90_TecMonty_aj.zip") as zf,
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        zf.extractall(tmpdir)
        df = gpd.read_file(Path(tmpdir) / "AGEBs 90_TecMonty_aj")
        df.columns = df.columns.str.lower()

    return (
        df.assign(
            cvegeo=lambda df: (
                df["cve_ent"].astype(str).str.zfill(2)
                + df["cve_mun"].astype(str).str.zfill(3)
                + df["cve_loc"].astype(str).str.zfill(4)
                + df["cve_ageb"].astype(str).str.zfill(4)
            ),
        )
        .drop(columns=["cve_ent", "cve_mun", "cve_loc", "cve_ageb", "objectid"])
        .to_crs("EPSG:6372")
    )
