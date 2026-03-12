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
import sqlalchemy

import dagster as dg
from census_processing.defs.resources import PathResource, PostGISResource


def cast_all_columns_to_numeric(
    df: pd.DataFrame | gpd.GeoDataFrame,
    ignore: Sequence[str] | None = None,
) -> pd.DataFrame | gpd.GeoDataFrame:
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
) -> dg.OpDefinition:
    @dg.op(name=f"census_{year}")
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

        return out.assign(
            ENTIDAD=lambda df: df["ENTIDAD"].astype(int).astype(str).str.zfill(2),
            MUN=lambda df: df["MUN"].astype(int).astype(str).str.zfill(3),
            LOC=lambda df: df["LOC"].astype(int).astype(str).str.zfill(4),
            AGEB=lambda df: df["AGEB"].astype(str).str.zfill(4),
            MZA=lambda df: df["MZA"].astype(str).str.zfill(3),
            CVEGEO=lambda df: df["ENTIDAD"]
            + df["MUN"]
            + df["LOC"]
            + df["AGEB"]
            + df["MZA"],
        )

    return _op


def extract_census_level_factory(
    level: Literal["ent", "mun", "loc", "ageb", "mza"],
) -> dg.OpDefinition:
    @dg.op(name=f"extract_census_{level}")
    def _op(df: pd.DataFrame) -> pd.DataFrame:
        if level == "ageb":
            df = df.query("MZA == '000'").assign(
                CVEGEO=lambda df: df["CVEGEO"].str[:-3]
            )
        elif level == "loc":
            df = df.query("MZA == '000' and AGEB == '0000'").assign(
                CVEGEO=lambda df: df["CVEGEO"].str[:-7]
            )
        elif level == "mun":
            df = df.query("MZA == '000' and AGEB == '0000' and LOC == '0000'").assign(
                CVEGEO=lambda df: df["CVEGEO"].str[:-11]
            )
        elif level == "ent":
            df = df.query(
                "MZA == '000' and AGEB == '0000' and LOC == '0000' and MUN == '000'"
            ).assign(CVEGEO=lambda df: df["CVEGEO"].str[:2])

        return df

    return _op


@dg.op
def remove_unused_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(
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
    )


extract_op_map = {
    "ent": extract_census_level_factory("ent"),
    "mun": extract_census_level_factory("mun"),
    "loc": extract_census_level_factory("loc"),
    "ageb": extract_census_level_factory("ageb"),
}


def add_derived_columns_factory(year: int) -> dg.OpDefinition:
    @dg.op(
        name=f"add_derived_columns_{year}",
    )
    def _op(df: pd.DataFrame) -> pd.DataFrame:
        with (
            Path(__file__).parent.parent.parent.parent
            / "config"
            / "derived_cols"
            / f"{year}.json"
        ).open() as f:
            column_expr: dict[str, str] = json.load(f)

        return df.assign(**{col: df.eval(expr) for col, expr in column_expr.items()})  # pyright: ignore[reportArgumentType]

    return _op


@dg.op(out=dg.Out(io_manager_key="geodataframe_postgis_manager"))
def merge_census_and_geometry(
    census: pd.DataFrame,
    geometry: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    return (
        geometry.merge(census, on="CVEGEO", how="inner")
        .pipe(
            cast_all_columns_to_numeric,
            ignore=["CVEGEO", "CVE_ENT", "CVE_MUN", "CVE_LOC", "CVE_AGEB", "geometry"],
        )
        .pipe(gpd.GeoDataFrame, geometry="geometry", crs=geometry.crs)
    )


def rename_columns_factory(year: int) -> dg.OpDefinition:
    @dg.op(
        name=f"rename_columns_{year}",
    )
    def _op(df: pd.DataFrame) -> pd.DataFrame:
        name_map_path = (
            Path(__file__).parent.parent.parent.parent
            / "config"
            / "column_maps"
            / f"{year}.json"
        )

        if not name_map_path.exists():
            return df

        with name_map_path.open(encoding="latin1") as f:
            column_map: dict = json.load(f)

        column_name_map = {
            i + 1: list(column_map.values())[i] for i in range(len(column_map))
        }
        wanted_cols = ["CVEGEO"] + [
            key for key, value in column_name_map.items() if value is not None
        ]

        return df[wanted_cols].rename(columns=column_name_map)

    return _op


@dg.op
def add_higher_levels_cvegeo(df: pd.DataFrame) -> pd.DataFrame:
    cvegeo_length = df["CVEGEO"].str.len().iloc[0]

    if cvegeo_length == 2:
        err = "CVEGEO is already at the highest level (entidad)"
        raise ValueError(err)

    df = df.assign(CVE_ENT=lambda df: df["CVEGEO"].str[:2])

    if cvegeo_length >= 9:
        df = df.assign(CVE_MUN=lambda df: df["CVEGEO"].str[:5])

    if cvegeo_length >= 13:
        df = df.assign(CVE_LOC=lambda df: df["CVEGEO"].str[:9])

    if cvegeo_length == 16:
        df = df.assign(CVE_AGEB=lambda df: df["CVEGEO"].str[:13])

    return df


def merged_factory(
    *,
    census_op: dg.OpDefinition,
    geometry_op_map: dict[str, dg.OpDefinition],
    year: int,
) -> dg.AssetsDefinition:
    @dg.graph_multi_asset(
        name=f"census_graph_{year}",
        outs={
            level: dg.AssetOut(
                key=["census", str(year), level],
                io_manager_key="geodataframe_postgis_manager",
                metadata={
                    "primary_key": "CVEGEO",
                    "table_name": f"census_{year}_{level}",
                },
                group_name=f"census_{year}",
            )
            for level in geometry_op_map
        },
        group_name=f"census_{year}",
    )
    def _asset() -> dict[str, gpd.GeoDataFrame]:
        census_orig = census_op()
        census_orig = rename_columns_factory(year)(census_orig)
        census_orig = add_derived_columns_factory(year)(census_orig)

        out = {}
        for level, geometry_op in geometry_op_map.items():
            if level != "mza":
                census = extract_op_map[level](census_orig)
            else:
                census = census_orig
            census = remove_unused_columns(census)

            if level != "ent":
                census = add_higher_levels_cvegeo(census)

            geometry = geometry_op()
            out[level] = merge_census_and_geometry(census, geometry)

        return out

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


@dg.asset(
    key=["census", "2020", "linked"],
    deps=[
        ["census", "2020", "ent"],
        ["census", "2020", "mun"],
        ["census", "2020", "loc"],
        ["census", "2020", "ageb"],
        ["census", "2020", "mza"],
    ],
    group_name="census_2020",
)
def link_cvegeos(postgis_resource: PostGISResource) -> dg.MaterializeResult:
    level_order = ["mza", "ageb", "loc", "mun", "ent"]

    for i in range(5):
        for j in range(i + 1, 5):
            fk_table = f"census_2020_{level_order[i]}"
            pk_table = f"census_2020_{level_order[j]}"
            col = f"CVE_{level_order[j].upper()}"
            constraint_name = f"fk_census_2020_{level_order[i]}_{level_order[j]}"

            with postgis_resource.connect() as conn:
                conn.execute(
                    sqlalchemy.text(
                        f"ALTER TABLE {fk_table} DROP CONSTRAINT IF EXISTS {constraint_name}"
                    )
                )
                conn.execute(
                    sqlalchemy.text(
                        f'ALTER TABLE {fk_table} ADD CONSTRAINT {constraint_name} FOREIGN KEY ("{col}") REFERENCES {pk_table} ("CVEGEO")'
                    )
                )
                conn.commit()
    return dg.MaterializeResult(["census", "2020", "linked"])
