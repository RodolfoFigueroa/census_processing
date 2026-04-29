import tempfile
import zipfile
from pathlib import Path
from typing import Literal

import geopandas as gpd

import dagster as dg
from census_processing.defs.assets.census_data.common import (
    full_census_2010_2020_factory,
    merged_factory,
)
from census_processing.defs.resources import PathResource


def geometry_2010_factory(level: Literal["ageb", "mun"]) -> dg.OpDefinition:
    suffix_map = {"ageb": ["au", "AGEB_urb"], "mun": ["m", "Municipios"]}

    @dg.op(name=f"geometry_2010_{level}")
    def _op(path_resource: PathResource) -> gpd.GeoDataFrame:
        raw_path = Path(path_resource.data_path) / "raws"

        with (
            tempfile.TemporaryDirectory() as tmpdir_1,
            tempfile.TemporaryDirectory() as tmpdir_2,
            zipfile.ZipFile(raw_path / "2010" / "702825292812_s.zip") as zf1,
        ):
            zf1.extractall(tmpdir_1)

            with zipfile.ZipFile(
                Path(tmpdir_1) / f"mg{suffix_map[level][0]}2010v5_0.zip"
            ) as zf2:
                zf2.extractall(tmpdir_2)

                df = gpd.read_file(
                    Path(tmpdir_2) / f"{suffix_map[level][1]}_2010_5.shp"
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


census_2010 = full_census_2010_2020_factory(
    year=2010,
    zip_template="resageburb_{i:02d}_2010_csv.zip",
    inner_dir_template="resultados_ageb_urbana_{i:02d}_cpv2010",
    csv_template="resultados_ageb_urbana_{i:02d}_cpv2010.csv",
)

data_2010 = merged_factory(
    census_op=census_2010,
    geometry_op_map={
        "mun": geometry_2010_factory("mun"),
        "ageb": geometry_2010_factory("ageb"),
    },
    year=2010,
)
