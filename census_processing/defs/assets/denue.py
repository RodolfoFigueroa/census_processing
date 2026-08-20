import tempfile
import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import get_args

import geopandas as gpd
from cfc_dagster_utils.types import (
    PostgresRelation,
    PostgresTableSpec,
    PostgresWriteMode,
)
from pyogrio.errors import DataSourceError, FeatureError

import dagster as dg
from census_processing.defs.assets.common import concat_geodataframes
from census_processing.defs.resources import PathResource
from census_processing.types import DenueYearsT

DENUE_DATES = get_args(DenueYearsT)

PREPARED_TABLE_SPEC_MAP = {
    key: PostgresTableSpec(
        relation=PostgresRelation(
            schema="staging",
            name=f"denue_{key}_prepared",
        ),
        write_mode=PostgresWriteMode.REPLACE,
        primary_key=("cvegeo",),
        geometry_column="geometry",
    )
    for key in DENUE_DATES
}


def get_denue_paths_factory(
    date: DenueYearsT,
) -> dg.OpDefinition:
    @dg.op(name=f"get_denue_paths_{date}", out=dg.DynamicOut())
    def _op(path_resource: PathResource) -> Iterator[dg.DynamicOutput[Path]]:
        denue_path = Path(path_resource.in_path) / "denue" / date

        if not denue_path.exists():
            err = f"Denue path {denue_path} does not exist"
            raise FileNotFoundError(err)

        for path in denue_path.glob("*.zip"):
            yield dg.DynamicOutput(path, mapping_key=path.stem.replace("-", "_"))

    return _op


get_denue_paths_op_map = {date: get_denue_paths_factory(date) for date in DENUE_DATES}


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


def denue_factory(date: DenueYearsT) -> dg.AssetsDefinition:
    @dg.graph_asset(
        key=["staging", "denue", date],
        metadata=PREPARED_TABLE_SPEC_MAP[date].to_dagster_metadata(),
        group_name="staging_denue",
    )
    def _asset() -> gpd.GeoDataFrame:
        denue_paths = get_denue_paths_factory(date)()
        return concat_geodataframes(denue_paths.map(process_denue_path).collect())

    return _asset


denue_assets = [denue_factory(date) for date in DENUE_DATES]
