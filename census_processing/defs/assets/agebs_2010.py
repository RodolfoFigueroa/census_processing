import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd

import dagster as dg
from census_processing.defs.assets.common import (
    full_census_2010_2020_factory,
)
from census_processing.defs.resources import PathResource


@dg.op(name="geometry_2010_ageb")
def geometry_2010_ageb(path_resource: PathResource) -> gpd.GeoDataFrame:
    raw_path = Path(path_resource.data_path) / "raws"

    with (
        tempfile.TemporaryDirectory() as tmpdir_1,
        tempfile.TemporaryDirectory() as tmpdir_2,
        zipfile.ZipFile(raw_path / "2010" / "702825292812_s.zip") as zf1,
    ):
        zf1.extractall(tmpdir_1)

        with zipfile.ZipFile(Path(tmpdir_1) / "mgau2010v5_0.zip") as zf2:
            zf2.extractall(tmpdir_2)

            return gpd.read_file(Path(tmpdir_2) / "AGEB_urb_2010_5.shp")[
                ["CVEGEO", "geometry"]
            ].set_crs("EPSG:6372", allow_override=True)


census_2010 = full_census_2010_2020_factory(
    year=2010,
    zip_template="resageburb_{i:02d}_2010_csv.zip",
    inner_dir_template="resultados_ageb_urbana_{i:02d}_cpv2010",
    csv_template="resultados_ageb_urbana_{i:02d}_cpv2010.csv",
)

# ageb_2010 = merged_factory(
#     census_op=census_2010,
#     geometry_op_map={"ageb": geometry_2010_ageb},
#     year=2010,
# )
