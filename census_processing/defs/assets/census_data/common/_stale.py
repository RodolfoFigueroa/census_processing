import os
import tempfile
import zipfile
from collections.abc import Sequence
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

import dagster as dg
from census_processing.constants import LEVEL_ORDER
from census_processing.defs.assets.census_data.common import (
    add_derived_columns_op_map,
    add_higher_levels_cvegeo,
    extract_op_map,
    merge_census_and_geometry,
    remove_unused_op_map,
    rename_columns_op_map,
)
from census_processing.defs.assets.metropoli import load_metropoli_df
from census_processing.defs.resources import PathResource


def read_census(
    fpath: os.PathLike,
    *,
    sep: str = ",",
    encoding: str = "utf-8",
) -> pd.DataFrame:
    """Reads and processes census data from a CSV file.

    This function reads census data from a CSV file, creates a geographic code
    (cvegeo) by concatenating state, municipality, and locality codes with
    zero-padding, removes geographic coordinate columns, and replaces common
    missing value indicators with NaN.

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
        raw_path = Path(path_resource.in_path)
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
    census_op_deps: dict[str, list[str]] | None = None,
) -> dg.AssetsDefinition:
    if census_op_deps is None:
        census_op_deps = {}

    @dg.graph_multi_asset(
        name=f"census_graph_{year}",
        ins={
            "df_metropoli_aggregated": dg.AssetIn(
                key=["metropoli", "2020"], dagster_type=dg.Nothing
            ),
            **{
                key: dg.AssetIn(key=value, dagster_type=dg.Nothing)
                for key, value in census_op_deps.items()
            },
        },
        outs={
            level: dg.AssetOut(
                key=["census", str(year), level],
                io_manager_key="postgres_manager",
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
    def _asset(df_metropoli_aggregated: None, **kwargs) -> dict[str, gpd.GeoDataFrame]:
        df_metropoli = load_metropoli_df()

        census_orig = census_op(**kwargs)
        census_orig = rename_columns_op_map[year](census_orig)
        census_orig = add_derived_columns_op_map[year](census_orig)

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

            # Since metropolitan zones are defined with 2020 muns we only link
            # them to said year's muns.
            if year == 2020 and level == "mun":
                census = add_metropoli_to_muns(
                    census,
                    df_metropoli,
                    df_metropoli_aggregated=df_metropoli_aggregated,
                )

            # For other years, we link the metropoli to the AGEBs directly based
            # on geometric intersections.
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
