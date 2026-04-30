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
from dagster_components.utils import cast_all_columns_to_numeric

import dagster as dg
from census_processing.constants import LEVEL_ORDER
from census_processing.defs.assets.metropoli import load_metropoli_df
from census_processing.defs.resources import PathResource


def read_census(
    fpath: os.PathLike,
    *,
    sep: str = ",",
    encoding: str = "utf-8",
) -> pd.DataFrame:
    """Reads and processes census data from a CSV file.

    This function reads census data from a CSV file, creates a geographic code (cvegeo)
    by concatenating state, municipality, and locality codes with zero-padding, removes
    geographic coordinate columns, and replaces common missing value indicators with NaN.

    Args:
        fpath: Path to the census CSV file.
        sep: Delimiter used in the CSV file. Defaults to ",".
        encoding: Character encoding of the CSV file. Defaults to "utf-8".

    Returns:
        pd.DataFrame: A processed census dataframe indexed by cvegeo (geographic code),
            sorted by index, with missing value indicators replaced by NaN.

    Raises:
        FileNotFoundError: If the file at fpath does not exist.
        pd.errors.ParserError: If the CSV file cannot be parsed correctly.
    """
    return (
        pd.read_csv(fpath, sep=sep, encoding=encoding)
        .assign(
            cvegeo=lambda df: (
                df["entidad"].astype(str).str.zfill(2)
                + df["mun"].astype(str).str.zfill(3)
                + df["loc"].astype(str).str.zfill(4)
            ),
        )
        .drop(columns=["longitud", "latitud", "altitud"])
        .set_index("cvegeo")
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

                try:
                    temp = pd.read_csv(
                        Path(tmpdir)
                        / inner_dir_template.format(i=i)
                        / "conjunto_de_datos"
                        / csv_template.format(i=i),
                    )
                except UnicodeDecodeError:
                    temp = pd.read_csv(
                        Path(tmpdir)
                        / inner_dir_template.format(i=i)
                        / "conjunto_de_datos"
                        / csv_template.format(i=i),
                        encoding="latin1",
                    )
                df_census.append(
                    temp.rename(
                        columns={"ï»¿ENTIDAD": "ENTIDAD", 'ï»¿"entidad"': "entidad"},
                        errors="ignore",
                    )
                )

        out = pd.concat(df_census, ignore_index=True)
        out.columns = out.columns.str.lower()

        return out.assign(
            entidad=lambda df: df["entidad"].astype(int).astype(str).str.zfill(2),
            mun=lambda df: df["mun"].astype(int).astype(str).str.zfill(3),
            loc=lambda df: df["loc"].astype(int).astype(str).str.zfill(4),
            ageb=lambda df: df["ageb"].astype(str).str.zfill(4),
            mza=lambda df: df["mza"].astype(str).str.zfill(3),
            cvegeo=lambda df: (
                df["entidad"] + df["mun"] + df["loc"] + df["ageb"] + df["mza"]
            ),
        )

    return _op


def extract_census_level_factory(
    level: Literal["ent", "mun", "loc", "ageb", "mza"],
) -> dg.OpDefinition:
    @dg.op(name=f"extract_census_{level}")
    def _op(df: pd.DataFrame) -> pd.DataFrame:
        if level == "ageb":
            df = df.query("mza == '000'").assign(
                cvegeo=lambda df: df["cvegeo"].str[:-3]
            )
        elif level == "loc":
            df = df.query("mza == '000' and ageb == '0000'").assign(
                cvegeo=lambda df: df["cvegeo"].str[:-7]
            )
        elif level == "mun":
            df = df.query("mza == '000' and ageb == '0000' and loc == '0000'").assign(
                cvegeo=lambda df: df["cvegeo"].str[:-11]
            )
        elif level == "ent":
            df = df.query(
                "mza == '000' and ageb == '0000' and loc == '0000' and mun == '000'"
            ).assign(cvegeo=lambda df: df["cvegeo"].str[:2])

        return df

    return _op


def remove_unused_columns_factory(
    level: Literal["ent", "mun", "loc", "other"],
) -> dg.OpDefinition:
    @dg.op(name=f"remove_unused_columns_{level}")
    def _op(df: pd.DataFrame) -> pd.DataFrame:
        out = df.drop(
            columns=[
                "entidad",
                "mun",
                "loc",
                "ageb",
                "mza",
            ],
            errors="ignore",
        )

        unwanted_names = {"nom_ent", "nom_mun", "nom_loc"} - {f"nom_{level.lower()}"}
        return out.drop(columns=unwanted_names, errors="ignore")

    return _op


extract_op_map = {
    "ent": extract_census_level_factory("ent"),
    "mun": extract_census_level_factory("mun"),
    "loc": extract_census_level_factory("loc"),
    "ageb": extract_census_level_factory("ageb"),
}

other_remove_op = remove_unused_columns_factory("other")
remove_unused_op_map = {
    "ent": remove_unused_columns_factory("ent"),
    "mun": remove_unused_columns_factory("mun"),
    "loc": remove_unused_columns_factory("loc"),
    "ageb": other_remove_op,
    "mza": other_remove_op,
}


def add_derived_columns_factory(year: int) -> dg.OpDefinition:
    @dg.op(
        name=f"add_derived_columns_{year}",
    )
    def _op(context: dg.OpExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
        with (
            Path(__file__).parent.parent.parent.parent.parent
            / "config"
            / "derived_cols"
            / f"{year}.json"
        ).open() as f:
            column_expr: dict[str, str] = json.load(f)

        return df.assign(**{col: df.eval(expr) for col, expr in column_expr.items()})  # ty:ignore[invalid-argument-type]

    return _op


@dg.op(
    ins={
        "census": dg.In(),
        "geometry": dg.In(),
        "table_ent": dg.In(dagster_type=dg.Nothing),
        "table_mun": dg.In(dagster_type=dg.Nothing),
        "table_loc": dg.In(dagster_type=dg.Nothing),
        "table_ageb": dg.In(dagster_type=dg.Nothing),
    },
    out=dg.Out(io_manager_key="geodataframe_postgis_manager"),
)
def merge_census_and_geometry(
    census: pd.DataFrame,
    geometry: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    return (
        geometry.merge(census, on="cvegeo", how="inner")
        .pipe(
            cast_all_columns_to_numeric,
            ignore=[
                "cvegeo",
                "cve_ent",
                "cve_mun",
                "cve_loc",
                "cve_ageb",
                "cve_met",
                "nom_ent",
                "nom_mun",
                "nom_loc",
                "geometry",
            ],
            errors="coerce",
        )
        .pipe(gpd.GeoDataFrame, geometry="geometry", crs=geometry.crs)
    )


def rename_columns_factory(year: int) -> dg.OpDefinition:
    @dg.op(
        name=f"rename_columns_{year}",
    )
    def _op(context: dg.OpExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
        name_map_path = (
            Path(__file__).parent.parent.parent.parent.parent
            / "config"
            / "column_maps"
            / f"{year}.json"
        )

        if not name_map_path.exists():
            msg = f"No column map found for year {year} at {name_map_path}, skipping renaming."
            context.log.warning(msg)
            return df

        with name_map_path.open(encoding="latin1") as f:
            column_map: dict = json.load(f)

        column_name_map = {
            i + 1: list(column_map.values())[i] for i in range(len(column_map))
        }
        wanted_cols = ["cvegeo"] + [
            key for key, value in column_name_map.items() if value is not None
        ]

        return df[wanted_cols].rename(columns=column_name_map)

    return _op


@dg.op
def add_higher_levels_cvegeo(df: pd.DataFrame) -> pd.DataFrame:
    cvegeo_length = df["cvegeo"].str.len().iloc[0]

    if cvegeo_length == 2:
        err = "cvegeo is already at the highest level (entidad)"
        raise ValueError(err)

    df = df.assign(cve_ent=lambda df: df["cvegeo"].str[:2])

    if cvegeo_length >= 9:
        df = df.assign(cve_mun=lambda df: df["cvegeo"].str[:5])

    if cvegeo_length >= 13:
        df = df.assign(cve_loc=lambda df: df["cvegeo"].str[:9])

    if cvegeo_length == 16:
        df = df.assign(cve_ageb=lambda df: df["cvegeo"].str[:13])

    return df


@dg.op(
    ins={
        "df_mun": dg.In(),
        "df_metropoli": dg.In(),
        "df_metropoli_aggregated": dg.In(dagster_type=dg.Nothing),
    }  # Dummy dependency to ensure metropoli asset is loaded before this op runs
)
def add_metropoli_to_muns(
    df_mun: pd.DataFrame, df_metropoli: gpd.GeoDataFrame
) -> pd.DataFrame:
    return df_mun.merge(df_metropoli[["cvegeo", "cve_met"]], how="left", on="cvegeo")


@dg.op(
    ins={
        "df_agebs": dg.In(),
        "df_metropoli": dg.In(),
        "df_metropoli_aggregated": dg.In(dagster_type=dg.Nothing),
    }  # Dummy dependency to ensure metropoli asset is loaded before this op runs)
)
def add_metropoli_to_agebs(
    df_agebs: gpd.GeoDataFrame, df_metropoli: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    overlay = (
        df_agebs[["cvegeo", "geometry"]]
        .assign(orig_area=lambda df: df["geometry"].area)
        .overlay(df_metropoli[["cve_met", "geometry"]], how="intersection")
        .assign(area_frac=lambda df: df["geometry"].area / df["orig_area"])
        .sort_values("area_frac", ascending=False)
        .drop_duplicates(subset=["cvegeo"], keep="first")
        .set_index("cvegeo")["cve_met"]
    )

    return df_agebs.set_index("cvegeo").assign(cve_met=overlay).reset_index()


def get_all_higher_levels(level: str) -> list[str]:
    if level not in LEVEL_ORDER:
        err = f"Invalid level: {level}. Must be one of {LEVEL_ORDER}."
        raise ValueError(err)

    level_idx = LEVEL_ORDER.index(level)
    return list(LEVEL_ORDER[level_idx + 1 :])


def generate_single_level_metadata(
    level: str, year: int, fk_levels: Sequence[str] | None = None
) -> dict[str, str | list[dict]]:
    if fk_levels is None:
        fk_levels = []

    out: dict[str, str | list[dict[str, str]]] = {
        "primary_key": "cvegeo",
        "table_name": f"census_{year}_{level}",
    }

    if level != "ent":
        out["foreign_keys"] = [
            {
                "column": f"cve_{fk_level}",
                "ref_column": "cvegeo",
                "ref_table": f"census_{year}_{fk_level}",
            }
            for fk_level in fk_levels
        ]

    curr_fk = out.get("foreign_keys", [])
    if not isinstance(curr_fk, list):
        err = f"Expected 'foreign_keys' to be a list, got {type(curr_fk)}"
        raise TypeError(err)

    if (year == 2020 and level == "mun") or (year != 2020 and level == "ageb"):
        out["foreign_keys"] = [
            *curr_fk,
            {
                "column": "cve_met",
                "ref_column": "cve_met",
                "ref_table": "metropoli_2020",
            },
        ]

    return out


def merged_factory(
    *,
    census_op: dg.OpDefinition,
    geometry_op_map: dict[str, dg.OpDefinition],
    year: int,
) -> dg.AssetsDefinition:
    @dg.graph_multi_asset(
        name=f"census_graph_{year}",
        ins={
            "df_metropoli_aggregated": dg.AssetIn(
                key=["metropoli", "2020"], dagster_type=dg.Nothing
            )
        },
        outs={
            level: dg.AssetOut(
                key=["census", str(year), level],
                io_manager_key="geodataframe_postgis_manager",
                metadata=generate_single_level_metadata(
                    level,
                    year,
                    fk_levels=get_all_higher_levels(level) if year == 2020 else None,
                ),
                group_name=f"census_{year}",
            )
            for level in LEVEL_ORDER
            if level in geometry_op_map
        },
        group_name=f"census_{year}",
    )
    def _asset(df_metropoli_aggregated: None) -> dict[str, gpd.GeoDataFrame]:
        df_metropoli = load_metropoli_df()

        census_orig = census_op()
        census_orig = rename_columns_factory(year)(census_orig)
        census_orig = add_derived_columns_factory(year)(census_orig)

        out = {}
        for level in reversed(LEVEL_ORDER):
            if level not in geometry_op_map:
                continue

            geometry = geometry_op_map[level]()

            if year in [2010, 2020] and level != "mza":
                census = extract_op_map[level](census_orig)
            else:
                census = census_orig

            census = remove_unused_op_map[level](census)

            if level != "ent":
                census = add_higher_levels_cvegeo(census)

            # Since metropolitan zones are defined with 2020 muns we only link them to said year's muns.
            if year == 2020 and level == "mun":
                census = add_metropoli_to_muns(
                    census,
                    df_metropoli,
                    df_metropoli_aggregated=df_metropoli_aggregated,
                )

            # For other years, we link the metropoli to the AGEBs directly based on geometric intersections.
            if year != 2020 and level == "ageb":
                geometry = add_metropoli_to_agebs(
                    geometry,
                    df_metropoli,
                    df_metropoli_aggregated=df_metropoli_aggregated,
                )

            # Add dependencies to higher levels if they exist, so that they are written
            # into the database sequentially, to avoid foreign key constraint issues.
            other_ops = {
                f"table_{key}": out[key] for key in out if key != level and key in out
            }
            out[level] = merge_census_and_geometry(census, geometry, **other_ops)

        return out

    return _asset


@dg.op(out=dg.Out(io_manager_key="geodataframe_postgis_manager"))
def add_dummy_geometry(df: pd.DataFrame) -> gpd.GeoDataFrame:
    return df.assign(
        geometry=lambda df: shapely.empty(
            len(df),
            geom_type=shapely.GeometryType.POLYGON,
        ),
    ).pipe(gpd.GeoDataFrame, geometry="geometry", crs="EPSG:6372")


@dg.op
def get_loc_geometry_from_agebs(ageb_geometries: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return (
        ageb_geometries.assign(cvegeo=lambda df: df["cvegeo"].str[:9])
        .dissolve(
            by="cvegeo",
        )[["geometry"]]
        .reset_index()
    )
