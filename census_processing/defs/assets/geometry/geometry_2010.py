import tempfile
import zipfile
from pathlib import Path
from typing import Literal

import geopandas as gpd

import dagster as dg
from census_processing.defs.resources import PathResource

SUFFIX_MAP = {
    "ent": ["e", "Entidades"],
    "mun": ["m", "Municipios"],
    "loc": ["lu", "Localidades_urbanas"],
    "ageb": ["au", "AGEB_urb"],
}


def geometry_2010_factory(
    level: Literal["ageb", "mun", "ent", "loc"],
) -> dg.AssetsDefinition:
    @dg.asset(
        key=["staging", "geometry", "2010", level],
        deps=[["input", "2010", "geometry"]],
        io_manager_key="postgres_manager",
        metadata={"schema": "staging", "table": f"geometry_2010_{level}_prepared"},
        group_name="staging/geometry/2010",
    )
    def _op(path_resource: PathResource) -> gpd.GeoDataFrame:
        raw_path = Path(path_resource.in_path)

        with (
            tempfile.TemporaryDirectory() as tmpdir_1,
            tempfile.TemporaryDirectory() as tmpdir_2,
            zipfile.ZipFile(raw_path / "2010" / "702825292812_s.zip") as zf1,
        ):
            zf1.extractall(tmpdir_1)

            with zipfile.ZipFile(
                Path(tmpdir_1) / f"mg{SUFFIX_MAP[level][0]}2010v5_0.zip"
            ) as zf2:
                zf2.extractall(tmpdir_2)

                df = gpd.read_file(
                    Path(tmpdir_2) / f"{SUFFIX_MAP[level][1]}_2010_5.shp"
                )
                df.columns = df.columns.str.lower()

                if "cvegeo" not in df.columns:
                    df = df.assign(
                        cvegeo=lambda df: (
                            df["cve_ent"].astype(int).astype(str).str.zfill(2)
                        )
                    )

                    if level in ["mun", "loc"]:
                        df = df.assign(
                            cvegeo=lambda df: (
                                df["cvegeo"]
                                + df["cve_mun"].astype(int).astype(str).str.zfill(3)
                            )
                        )

                    # AGEBs already have a cvegeo

                    if level == "loc":
                        df = df.assign(
                            cvegeo=lambda df: (
                                df["cvegeo"]
                                + df["cve_loc"].astype(int).astype(str).str.zfill(4)
                            )
                        )

                return df[["cvegeo", "geometry"]].set_crs(
                    "EPSG:6372", allow_override=True
                )

    return _op


geometry_2020_assets = [
    geometry_2010_factory(level) for level in ("ent", "mun", "loc", "ageb")
]
