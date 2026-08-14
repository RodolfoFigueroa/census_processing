import io
import tempfile
import zipfile
from pathlib import Path

import cabarchive
import geopandas as gpd
import numpy as np
import pandas as pd
import rarfile
from cfc_dagster_utils.types import (
    PostgresRelation,
    PostgresTableSpec,
    PostgresWriteMode,
)

import dagster as dg
from census_processing.defs.assets.census_data.common import (
    add_derived_columns_factory,
    add_higher_levels_cvegeo,
    cast_all_columns_to_numeric,
    merge_census_and_geometry,
    remove_unused_op_map,
    rename_columns_factory,
)
from census_processing.defs.resources import PathResource


def get_names_from_archive(arc: cabarchive.CabArchive) -> list[str]:
    """
    Extract and clean the variable names from the archive.

    Args:
        arc: The CAB archive object.

    Returns:
        A list of cleaned variable names.
    """
    names = None
    for key in arc:
        if "CGPV2000" in key:
            names = (
                arc[key]
                .buf.decode("latin1")
                .replace("\x03e", "")
                .replace("\x1bª", "")
                .replace("\x00", "")
                .replace("\x1a", "")
                .replace("¢", "ó")
                .replace("¤", "ñ")
                .replace("\xa0", "á")
                .replace("\x82", "é")
                .strip()
            )
            break

    if names is None:
        err = "CGPV2000 file not found in archive"
        raise ValueError(err)

    split = names.split("\r")[1].strip()
    return [split[353 * i : 353 * (i + 1)].strip() for i in range(170)]


def decode_chunk(buffer: bytes) -> str:
    """
    Decode a chunk of bytes into a clean string.

    Args:
        buffer: The byte buffer to decode.

    Returns:
        The cleaned string.
    """
    return (
        buffer.decode("latin1")
        .split("\n")[-1]
        .replace("\x00", "")
        .replace("\x1a", "")
        .replace("\r", "")
        .strip()
    )


def fix_merged_element(elem: str, affix: str) -> list:
    """
    Fix elements that have merged with affixes like "N.D." or "*".

    Args:
        elem: The element to fix.
        affix: The affix to split on.

    Returns:
        A list of fixed elements, with np.nan for missing values.
    """
    out = []
    if elem == affix:
        return [np.nan]

    split = elem.split(affix)
    for s in split:
        if len(s) == 0:
            out.append(np.nan)
        else:
            out.append(s)
    return out


def process_chunk(chunk: str) -> list[str]:
    """
    Process a chunk of data, handling special cases.

    Args:
        chunk: The chunk of data to process.

    Returns:
        A list of processed elements.
    """
    elems = []
    for elem in chunk.split():
        if "-" in elem:
            a, b = elem.split("-")
            ageb_code = a + b[0]
            z1 = b[1:]
            elems.append(ageb_code)
            elems.append(z1)
        elif "N.D." in elem:
            fixed = fix_merged_element(elem, "N.D.")
            elems.extend(fixed)
        elif "*" in elem:
            fixed = fix_merged_element(elem, "*")
            elems.extend(fixed)
        else:
            elems.append(elem)

    return [x for x in elems if x != ""]


def chunk_to_dataframe(rows: list[str]) -> pd.DataFrame:
    """
    Convert a list of rows into a structured DataFrame.

    Args:
        rows: The list of rows to convert.

    Returns:
        The structured DataFrame.
    """
    if len(rows) % 171 != 0:
        err = "Number of elements is not a multiple of 171"
        raise ValueError(err)

    return (
        pd.Series(rows)
        .to_frame()
        .assign(
            row_idx=lambda df: df.index // 171,
            col_idx=lambda df: df.index % 171,
        )
        .pivot_table(index="row_idx", columns="col_idx", values=0, aggfunc="first")
        .set_index(
            0,
        )
    )


def extract_from_archive(arc: cabarchive.CabArchive) -> pd.DataFrame:
    """
    Extract and process census data from a CAB archive.

    Args:
        arc: The CAB archive object.

    Returns:
        The processed census DataFrame.
    """

    wanted_keys = []
    for elem in arc:
        if elem.endswith(".DBP"):
            suffix = elem.split("_")[-1].replace(".DBP", "")
            if len(suffix) > 2:
                wanted_keys.append(elem)

    df_census = []
    for key in wanted_keys:
        loc_code = key.split("_")[-1].replace(".DBP", "")
        buf = arc[key].buf
        decoded = decode_chunk(buf)
        processed = process_chunk(decoded)
        df_chunk = chunk_to_dataframe(processed)
        df_chunk.index = loc_code + df_chunk.index.astype(str)
        df_census.append(df_chunk)

    return pd.concat(df_census)


@dg.op(name="census_2000_ageb", ins={"demography": dg.In(dagster_type=dg.Nothing)})
def census_2000_ageb(path_resource: PathResource) -> pd.DataFrame:
    raw_path = Path(path_resource.data_path) / "input"

    df_census_list: list[pd.DataFrame] = []
    with (
        zipfile.ZipFile(raw_path / "2000" / "censo2000_scince.zip") as f,
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        f.extractall(tmpdir)
        extracted_path = Path(tmpdir) / "censo2000_scince"

        for rar in extracted_path.glob("*.rar"):
            with (
                rarfile.RarFile(rar) as rf,
                tempfile.TemporaryDirectory() as rar_tmpdir,
            ):
                rf.extractall(rar_tmpdir)

                cab_path = Path(rar_tmpdir) / rar.stem / "Data.Cab"
                with cab_path.open("rb") as cabf:
                    arc = cabarchive.CabArchive(cabf.read())

            df_census_list.append(extract_from_archive(arc))

    df_census = pd.concat(df_census_list)
    return (
        df_census.sort_index()
        .assign(ageb_id=lambda df: df.index.str.slice(9, 13))
        .query("ageb_id != '0000'")
        .drop(columns=["ageb_id"])
        .pipe(cast_all_columns_to_numeric)
        .reset_index(names="cvegeo")
    )


@dg.op(name="geometry_2000_ageb", ins={"geometry": dg.In(dagster_type=dg.Nothing)})
def geometry_2000_ageb(path_resource: PathResource) -> gpd.GeoDataFrame:
    raw_path = Path(path_resource.data_path) / "input"

    with (
        zipfile.ZipFile(raw_path / "2000" / "702825292843_s.zip") as f,
        f.open("mgau2000.zip") as subzip,
    ):
        subzip_data = io.BytesIO(subzip.read())
        with (
            zipfile.ZipFile(subzip_data) as f_nested,
            tempfile.TemporaryDirectory() as tmpdir,
        ):
            f_nested.extractall(tmpdir)
            df_geom = gpd.read_file(tmpdir)

    df_geom.columns = df_geom.columns.str.lower()

    return df_geom.assign(cvegeo=lambda df: df["clvagb"].str.replace("-", ""))[
        ["cvegeo", "geometry"]
    ].to_crs("EPSG:6372")


PREPARED_TABLE_SPEC = PostgresTableSpec(
    relation=PostgresRelation(
        schema="staging",
        name="census_2000_ageb_prepared",
    ),
    write_mode=PostgresWriteMode.REPLACE,
    primary_key=("cvegeo",),
    geometry_column="geometry",
)


@dg.graph_asset(
    key=["census", "2000", "ageb_prepared"],
    ins={
        "demography": dg.AssetIn(
            key=["input", "2000", "demography"], dagster_type=dg.Nothing
        ),
        "geometry": dg.AssetIn(
            key=["input", "2000", "geometry", "ageb"], dagster_type=dg.Nothing
        ),
    },
    metadata=PREPARED_TABLE_SPEC.to_dagster_metadata(),
    group_name="census_2000",
)
def census_graph_2000(demography: None, geometry: None) -> gpd.GeoDataFrame:
    census = census_2000_ageb(demography)
    census = rename_columns_factory(2000)(census)
    census = add_derived_columns_factory(2000)(census)

    geometry = geometry_2000_ageb(geometry)

    census = remove_unused_op_map["ageb"](census)
    census = add_higher_levels_cvegeo(census)

    return merge_census_and_geometry(census, geometry)


# census_2000_non_agebs = census_1990_2000_factory(
#     compressed_path=Path("2000", "cgpv2000_iter_00_csv.zip"),
#     extracted_path=Path(
#         "cgpv2000_iter_00",
#         "conjunto_de_datos",
#         "cgpv2000_iter_00.csv",
#     ),
#     year=2000,
#     encoding="utf-8",
#     sep=",",
# )
