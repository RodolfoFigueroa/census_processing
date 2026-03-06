import json
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


def cast_all_columns_to_numeric(
    df: pd.DataFrame,
    ignore: Sequence[str] | None = None,
) -> pd.DataFrame:
    if ignore is None:
        ignore = []

    df = df.copy()
    for col in df.columns:
        if col not in ignore:
            df[col] = pd.to_numeric(df[col], errors="coerce")
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


def census_1990_2000_factory(
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


def full_census_2010_2020_factory(
    *,
    year: int,
    zip_template: str,
    inner_dir_template: str,
    csv_template: str,
    level: Literal["ent", "mun", "loc", "ageb", "mza"],
) -> dg.OpDefinition:
    @dg.op(name=f"census_{year}_{level}")
    def _op(path_resource: PathResource) -> pd.DataFrame:
        raw_path = Path(path_resource.data_path) / "raws"

        df_census: list[pd.DataFrame] = []

        for i in range(1, 33):
            compressed_path = raw_path / str(year) / "census" / zip_template.format(i=i)

            with (
                tempfile.TemporaryDirectory() as tmpdir,
                zipfile.ZipFile(compressed_path) as zf,
            ):
                zf.extractall(tmpdir)

                df_census.append(
                    pd.read_csv(
                        Path(tmpdir)
                        / inner_dir_template.format(i=i)
                        / "conjunto_de_datos"
                        / csv_template.format(i=i),
                        encoding="latin1",
                    ).rename(
                        columns={"ï»¿ENTIDAD": "ENTIDAD", 'ï»¿"entidad"': "entidad"},
                        errors="ignore",
                    )
                )

        out = pd.concat(df_census, ignore_index=True)
        out.columns = [c.upper() for c in out.columns]

        out = out.assign(
            CVEGEO=lambda df: df["ENTIDAD"].astype(int).astype(str).str.zfill(2)
            + df["MUN"].astype(int).astype(str).str.zfill(3)
            + df["LOC"].astype(int).astype(str).str.zfill(4)
            + df["AGEB"].astype(str).str.zfill(4)
            + df["MZA"].astype(str).str.zfill(3),
        )

        if level == "ageb":
            out = out.query("MZA == 0").assign(CVEGEO=lambda df: df["CVEGEO"].str[:-3])
        elif level == "loc":
            out = out.query("MZA == 0 and AGEB == 0").assign(
                CVEGEO=lambda df: df["CVEGEO"].str[:-7]
            )
        elif level == "mun":
            out = out.query("MZA == 0 and AGEB == 0 and LOC == 0").assign(
                CVEGEO=lambda df: df["CVEGEO"].str[:-11]
            )
        elif level == "ent":
            out = out.query("MZA == 0 and AGEB == 0 and LOC == 0 and MUN == 0").assign(
                CVEGEO=lambda df: df["CVEGEO"].str[:2]
            )

        return out.drop(
            columns=[
                "ENTIDAD",
                "MUN",
                "LOC",
                "AGEB",
                "MZA",
                "NOM_ENT",
                "NOM_MUN",
                "NOM_LOC",
            ],
            errors="ignore",
        ).pipe(cast_all_columns_to_numeric, ignore=["CVEGEO"])

    return _op


@dg.op
def merge_census_and_geometry(
    census: pd.DataFrame,
    geometry: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        geometry.merge(census, on="CVEGEO", how="inner"),
    ).sort_values("CVEGEO")


@dg.op(out=dg.Out(io_manager_key="geodataframe_postgis_manager"))
def dummy_op(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return df


def merged_factory(
    *,
    census_op: dg.OpDefinition,
    geometry_op: dg.OpDefinition,
    year: int,
    rename_op: dg.OpDefinition | None = None,
    derived_cols_op: dg.OpDefinition | None = None,
    level: Literal["ageb", "mza"],
) -> dg.AssetsDefinition:
    @dg.graph_asset(
        key=["census", str(year), level],
        metadata={
            "table_name": f"census_{year}_{level}",
        },
        group_name=f"census_{year}",
    )
    def _asset() -> gpd.GeoDataFrame:
        census = census_op()
        geometry = geometry_op()
        merged = merge_census_and_geometry(census, geometry)

        if rename_op is not None:
            merged = rename_op(merged)

        if derived_cols_op is None:
            return dummy_op(merged)

        return derived_cols_op(merged)

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


@dg.op
def get_loc_geometry_from_agebs(ageb_geometries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return (
        ageb_geometries.assign(CVEGEO=lambda df: df["CVEGEO"].str[:9])
        .dissolve(
            by="CVEGEO",
        )[["geometry"]]
        .reset_index()
    )


def rename_columns_factory(year: int) -> dg.OpDefinition:
    @dg.op(
        name=f"rename_columns_{year}",
    )
    def _op(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        with (
            Path(__file__).parent.parent.parent.parent
            / "config"
            / "column_maps"
            / f"{year}.json"
        ).open(encoding="latin1") as f:
            column_map: dict = json.load(f)

        column_name_map = {
            i + 1: list(column_map.values())[i] for i in range(len(column_map))
        }
        wanted_cols = (
            ["CVEGEO"]
            + [key for key, value in column_name_map.items() if value is not None]
            + ["geometry"]
        )

        return (
            df[wanted_cols]
            .rename(columns=column_name_map)
            .pipe(gpd.GeoDataFrame, geometry="geometry", crs=df.crs)
        )

    return _op


def add_derived_columns_factory(year: int) -> dg.OpDefinition:
    @dg.op(
        name=f"add_derived_columns_{year}",
        out=dg.Out(io_manager_key="geodataframe_postgis_manager"),
    )
    def _op(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        with (
            Path(__file__).parent.parent.parent.parent
            / "config"
            / "derived_cols"
            / f"{year}.json"
        ).open() as f:
            column_expr: dict[str, str] = json.load(f)

        return df.assign(**{col: df.eval(expr) for col, expr in column_expr.items()})  # pyright: ignore[reportArgumentType]

    return _op
