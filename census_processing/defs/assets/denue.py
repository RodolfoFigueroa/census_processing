import tempfile
import zipfile
from collections.abc import Iterator
from pathlib import Path

import geopandas as gpd
from pyogrio.errors import DataSourceError, FeatureError

import dagster as dg
from census_processing.defs.assets.common import concat_geodataframes
from census_processing.defs.resources import PathResource


def get_denue_paths_factory(date: str) -> dg.OpDefinition:
    @dg.op(name=f"get_denue_paths_{date}", out=dg.DynamicOut())
    def _op(path_resource: PathResource) -> Iterator[dg.DynamicOutput[Path]]:
        denue_path = Path(path_resource.data_path) / "input" / "denue" / date
        for path in denue_path.glob("*.zip"):
            yield dg.DynamicOutput(path, mapping_key=path.stem.replace("-", "_"))

    return _op


def read_df_fallback(path: Path) -> gpd.GeoDataFrame:
    try:
        return gpd.read_file(path)
    except FeatureError:
        # Fall back to fiona if pyogrio fails, as some files have DBF issues
        return gpd.read_file(path, engine="fiona")


@dg.op
def process_denue_path(path: Path) -> gpd.GeoDataFrame:
    with zipfile.ZipFile(path) as zf, tempfile.TemporaryDirectory() as tmpdir:
        zf.extractall(tmpdir)
        tmp_path = Path(tmpdir)

        try:
            out = read_df_fallback(tmp_path / "conjunto_de_datos")
        except DataSourceError as e:
            # Some files have an extra directory level
            dirs_in_tmp = list(tmp_path.iterdir())
            if len(dirs_in_tmp) == 1 and dirs_in_tmp[0].is_dir():
                out = read_df_fallback(dirs_in_tmp[0] / "conjunto_de_datos")
            else:
                err = f"Could not find 'conjunto_de_datos' in {path}"
                raise DataSourceError(err) from e

        out.columns = out.columns.str.lower()
        return out.drop(
            columns=[
                "latitud",
                "longitud",
                "cve_ent",
                "cve_mun",
                "cve_loc",
                "ageb",
                "manzana",
            ]
        ).to_crs("EPSG:6372")


def denue_factory(date: str) -> dg.AssetsDefinition:
    @dg.graph_asset(
        key=["denue", date],
        metadata={"table_name": f"denue_{date}"},
        group_name="denue",
    )
    def _asset() -> gpd.GeoDataFrame:
        denue_paths = get_denue_paths_factory(date)()
        return concat_geodataframes(denue_paths.map(process_denue_path).collect())

    return _asset


denue_assets = [
    denue_factory(date)
    for date in ["2020_11", "2021_11", "2022_11", "2023_11", "2024_11", "2025_05"]
]
