import os
import tempfile
import zipfile
from collections.abc import Sequence
from pathlib import Path
from typing import Literal

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

import dagster as dg
from census_processing.defs.resources import PathResource


def cast_to_numeric(
    df: pd.DataFrame,
    ignore: Sequence[str] | None = None,
) -> pd.DataFrame:
    if ignore is None:
        ignore = []

    df = df.copy()
    for col in df.columns:
        if col not in ignore:
            df[col] = pd.to_numeric(df[col], errors="raise")
    return df


def read_census(
    fpath: os.PathLike,
    *,
    sep: str = ",",
    encoding: str = "utf-8",
) -> pd.DataFrame:
    return (
        pd.read_csv(fpath, sep=sep, encoding=encoding)
        .assign(
            CVEGEO=lambda df: df["entidad"].astype(str).str.zfill(2)
            + df["mun"].astype(str).str.zfill(3)
            + df["loc"].astype(str).str.zfill(4),
        )
        .drop(columns=["longitud", "latitud", "altitud"])
        .set_index("CVEGEO")
        .sort_index()
        .replace(["*", "N.D.", "N/D"], np.nan)
    )


def load_census_df_factory(
    *,
    compressed_path: os.PathLike,
    extracted_path: os.PathLike,
    year: int,
    sep: str,
    encoding: str,
) -> dg.OpDefinition:
    @dg.op(name=f"load_census_{year}_df")
    def _op(path_resource: PathResource) -> pd.DataFrame:
        raw_path = Path(path_resource.data_path) / "raws"
        fpath_compressed = raw_path / compressed_path

        with (
            tempfile.TemporaryDirectory() as tmpdir,
            zipfile.ZipFile(fpath_compressed) as zf,
        ):
            zf.extractall(tmpdir)
            return read_census(
                Path(tmpdir) / extracted_path,
                sep=sep,
                encoding=encoding,
            )

    return _op


def extract_census_level_factory(
    level: Literal["ent", "mun", "loc"],
) -> dg.OpDefinition:
    @dg.op(name=f"extract_census_{level}_level")
    def _op(census: pd.DataFrame) -> pd.DataFrame:
        if level == "loc":
            cutoff = 9
            out = census.query("(loc != 0) & (mun != 0) & (entidad != 0)")
        elif level == "mun":
            cutoff = 5
            out = census.query("(loc == 0) & (mun != 0) & (entidad != 0)")
        elif level == "ent":
            cutoff = 2
            out = census.query("(loc == 0) & (mun == 0) & (entidad != 0)")

        out = (
            out.rename(columns={f"nom_{level}": "nombre"})
            .drop(
                columns=list(
                    {"entidad", "mun", "loc", "nom_ent", "nom_mun", "nom_loc"}
                    - {f"nom_{level}"},  # Keep the name of the wanted level
                ),
            )
            .assign(CVEGEO=lambda df: df.index.str.slice(0, cutoff))
            .reset_index(drop=True)
        )
        out = cast_to_numeric(out, ignore=["nombre", "CVEGEO"])
        return pd.DataFrame(
            out[["CVEGEO"] + [col for col in out.columns if col != "CVEGEO"]],
        )

    return _op


@dg.op(out=dg.Out(io_manager_key="geodataframe_postgis_manager"))
def merge_census_and_geometry(
    census: pd.DataFrame,
    geometry: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(geometry.merge(census, on="CVEGEO", how="inner"))


def merged_factory(
    *,
    census_op: dg.OpDefinition,
    geometry_op: dg.OpDefinition,
    year: int,
) -> dg.AssetsDefinition:
    @dg.graph_asset(
        name="ageb",
        key_prefix=["census", str(year)],
        metadata={
            "table_name": f"census_{year}_ageb",
        },
        group_name=f"census_{year}",
    )
    def _asset() -> gpd.GeoDataFrame:
        census = census_op()
        geometry = geometry_op()
        return merge_census_and_geometry(census, geometry)

    return _asset


@dg.op(out=dg.Out(io_manager_key="geodataframe_postgis_manager"))
def add_dummy_geometry(df: pd.DataFrame) -> gpd.GeoDataFrame:
    df = df.assign(
        geometry=lambda df: shapely.empty(
            len(df),
            geom_type=shapely.GeometryType.POLYGON,
        ),
    )
    return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:6372")


extract_loc = extract_census_level_factory(level="loc")
extract_mun = extract_census_level_factory(level="mun")
extract_ent = extract_census_level_factory(level="ent")


@dg.op
def get_loc_geometry_from_agebs(ageb_geometries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return (
        ageb_geometries.assign(CVEGEO=lambda df: df["CVEGEO"].str[:9])
        .dissolve(
            by="CVEGEO",
        )[["geometry"]]
        .reset_index()
    )


def multi_merged_factory(*, year: int, df_op: dg.OpDefinition) -> dg.AssetsDefinition:
    @dg.graph_multi_asset(
        ins={
            "agebs": dg.AssetIn(key=["census", str(year), "ageb"]),
        },
        outs={
            "loc": dg.AssetOut(
                key=["census", str(year), "loc"],
                group_name=f"census_{year}",
                is_required=True,
                metadata={"table_name": f"census_{year}_loc"},
            ),
            "mun": dg.AssetOut(
                key=["census", str(year), "mun"],
                group_name=f"census_{year}",
                is_required=True,
                metadata={"table_name": f"census_{year}_mun"},
            ),
            "ent": dg.AssetOut(
                key=["census", str(year), "ent"],
                group_name=f"census_{year}",
                is_required=True,
                metadata={"table_name": f"census_{year}_ent"},
            ),
        },
        can_subset=False,
        group_name=f"census_{year}",
    )
    def _asset(
        agebs: gpd.GeoDataFrame,
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
        census_df = df_op()
        loc_geometries = get_loc_geometry_from_agebs(agebs)
        loc_census = extract_loc(census_df)

        return (
            merge_census_and_geometry(loc_census, loc_geometries),
            add_dummy_geometry(extract_mun(census_df)),
            add_dummy_geometry(extract_ent(census_df)),
        )

    return _asset
